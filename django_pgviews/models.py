import logging
import string

from django.apps import apps
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import connection

from django_pgviews.view import create_view, View, MaterializedView
from django_pgviews.signals import view_synced, all_views_synced

log = logging.getLogger("django_pgviews.sync_pgviews")


class ViewSyncer(object):
    def run(self, force, update, **options):
        self.synced = []
        backlog = []
        for view_cls in apps.get_models():
            if not (isinstance(view_cls, type) and issubclass(view_cls, View) and hasattr(view_cls, "sql")):
                continue
            backlog.append(view_cls)
        loop = 0
        while len(backlog) > 0 and loop < 10:
            loop += 1
            backlog = self.run_backlog(backlog, force, update)

        if loop >= 10:
            log.warning("pgviews dependencies hit limit. Check if your model dependencies are correct")
        else:
            all_views_synced.send(sender=None)

    def run_backlog(self, models, force, update):
        """Installs the list of models given from the previous backlog

        If the correct dependent views have not been installed, the view
        will be added to the backlog.

        Eventually we get to a point where all dependencies are sorted.
        """
        backlog = []
        for view_cls in models:
            skip = False
            name = "{}.{}".format(view_cls._meta.app_label, view_cls.__name__)
            for dep in view_cls._dependencies:
                if dep not in self.synced:
                    skip = True
            if skip is True:
                backlog.append(view_cls)
                log.info("Putting pgview at back of queue: %s", name)
                continue  # Skip

            try:
                app_label = ContentType.objects.get_for_model(view_cls).app_label
                if hasattr(settings, "TENANT_APPS") and app_label in settings.TENANT_APPS:
                    from tenant_schemas.utils import get_public_schema_name, get_tenant_model, schema_exists

                    tenants = (
                        get_tenant_model()
                        .objects.exclude(schema_name=get_public_schema_name())
                        .values_list("schema_name", flat=True)
                    )
                else:
                    tenants = ["public"]
                status = "EXISTS"
                for tenant in tenants:
                    try:
                        connection.set_schema(tenant)
                        log.info("Switched to %s schema for %s", tenant, view_cls._meta.db_table)
                    except:
                        pass
                    status = create_view(
                        connection,
                        view_cls._meta.db_table,
                        string.Template(view_cls.sql).safe_substitute(tenant=tenant),
                        update=update,
                        force=force,
                        materialized=isinstance(view_cls(), MaterializedView),
                        index=view_cls._concurrent_index,
                        column_indexes=view_cls._column_indexes,
                        tenant_schema=tenant,
                    )
                    try:
                        connection.set_schema_to_public()
                    except:
                        pass

                view_synced.send(
                    sender=view_cls,
                    update=update,
                    force=force,
                    status=status,
                    has_changed=status not in ("EXISTS", "FORCE_REQUIRED"),
                )
                self.synced.append(name)
            except Exception as exc:
                exc.view_cls = view_cls
                exc.python_name = name
                raise
            else:
                if status == "CREATED":
                    msg = "created"
                elif status == "UPDATED":
                    msg = "updated"
                elif status == "EXISTS":
                    msg = "already exists, skipping"
                elif status == "FORCED":
                    msg = "forced overwrite of existing schema"
                elif status == "FORCE_REQUIRED":
                    msg = "exists with incompatible schema, " "--force required to update"
                log.info("pgview %(python_name)s %(msg)s" % {"python_name": name, "msg": msg})
        return backlog
