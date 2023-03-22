import logging

import django_filters
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.http import Http404
from rest_framework import serializers, viewsets
from rest_framework.decorators import action
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response

from companies.mixins import ProjectCompanyMixin
from seo.api.pages import CuttedPageSerializer
from seo.models import Project, Query, Position, Log
from seo.tasks import recalc_page, heat_project_cache

log = logging.getLogger(__name__)


class CuttedPositionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Position
        fields = ('id', 'position', 'real_url')


class QuerySerializer(serializers.ModelSerializer):
    page_id = serializers.IntegerField(required=False, write_only=True, allow_null=True)
    page = CuttedPageSerializer(read_only=True)
    actual_position = CuttedPositionSerializer(read_only=True)

    class Meta:
        model = Query
        fields = ['id', 'page_id', 'page', 'query', 'project', 'is_deleted',
                  'frequency', 'frequency_quotes', 'frequency_quotes_exact',
                  'actual_position', 'prev_position',
                  ]


class QueriesFilter(django_filters.Filter):
    def filter(self, qs, value):
        if not value:
            return qs

        queries = (value or '').split(',')
        r = cache.client.connect()

        ids_include = []
        ids_exclude = []
        for word in queries:
            if word.startswith('!'):
                word = word.replace('!', '').strip()

                key = f"{settings.QUERYINDEX_PREFIX}_{word}"
                ids = r.smembers(key)

                ids_include.append(ids)
                log.info("Part %s include %d", word, len(ids))

            elif word.startswith('-'):
                word = word.replace('-', '').strip()

                keys = r.keys(f"{settings.QUERYINDEX_PREFIX}_{word}*")
                if not keys:
                    continue

                ids = r.sunion(*keys)

                ids_exclude.append(ids)
                log.info("Part %s exclude %d", word, len(ids))

            else:
                word = word.strip()

                keys = r.keys(f"{settings.QUERYINDEX_PREFIX}_{word}*")
                if not keys:
                    continue

                ids = r.sunion(*keys)

                ids_include.append(ids)
                log.info("Part %s include %d", word, len(ids))

        if ids_include:
            good_ids = set.intersection(*ids_include)

            if ids_exclude:
                bad_ids = set.union(*ids_exclude)
                good_ids -= bad_ids

            log.info("Filter queries by ids %d", len(good_ids))
            return qs.filter(id__in=good_ids)

        else:
            bad_ids = set.union(*ids_exclude)
            log.info("Exclude queries by ids %d", len(bad_ids))
            return qs.exclude(id__in=bad_ids)


class QueryFilter(django_filters.FilterSet):
    page__isnull = django_filters.NumberFilter(field_name='page', lookup_expr='isnull')
    queries = QueriesFilter()

    class Meta:
        model = Query
        fields = ['project', 'page', 'page_id', 'page__isnull', 'is_deleted', 'queries', 'query']
        order_by = ('-frequency', 'query')


class LargeResultsSetPagination(LimitOffsetPagination):
    default_limit = 500
    max_limit = 500


class QueryViewSet(ProjectCompanyMixin, viewsets.ModelViewSet):
    queryset = Query.objects.all()
    serializer_class = QuerySerializer
    filterset_class = QueryFilter
    pagination_class = LargeResultsSetPagination

    @action(detail=False, methods=['post'])
    def upload(self, request):
        try:
            project = Project.objects.get(pk=request.POST.get('project'))
        except Project.DoesNotExist:
            raise Http404

        queries = request.POST.get('queries', '')
        for query in queries.split('\n'):
            query = query.strip()
            if query:
                q, created = Query.objects.get_or_create(
                    project=project,
                    query=query,
                )

                if created:
                    Log.objects.create(
                        actor=request.user,
                        action=Log.QUERY_CREATE,
                        query=q,
                        project=project,
                    )

        transaction.on_commit(lambda: heat_project_cache.delay(project.id))

        return Response('', status=201)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        old_page = instance.page
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)

        if 'is_deleted' in request.data and instance.is_deleted != request.data['is_deleted']:
            if instance.is_deleted:
                action = Log.QUERY_UNDELETE
            else:
                action = Log.QUERY_DELETE

            Log.objects.create(
                actor=request.user,
                action=action,
                query=instance,
                project=instance.project,
            )

            instance.is_deleted = request.data['is_deleted']
            instance.save()

        elif 'page_id' in request.data and request.data['page_id'] != instance.page_id:
            instance = serializer.save()

            if old_page:
                Log.objects.create(
                    actor=request.user,
                    action=Log.GROUP_SHRINK,
                    query=instance,
                    project=instance.project,
                    page=old_page,
                )
                recalc_page.delay(old_page.pk)

            if instance.page_id:
                Log.objects.create(
                    actor=request.user,
                    action=Log.GROUP_EXPAND,
                    query=instance,
                    project=instance.project,
                    page_id=instance.page_id,
                )
                recalc_page.delay(instance.page_id)

        return Response(serializer.data)
