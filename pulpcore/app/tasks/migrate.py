import logging
from gettext import gettext as _

from django.utils.timezone import now

from pulpcore.app.models import Artifact, ProgressReport, storage
from pulpcore.app.serializers import DomainBackendMigratorSerializer
from pulpcore.app.util import get_domain

_logger = logging.getLogger(__name__)


def migrate_backend(data):
    """
    Copy the artifacts from the current storage backend to a new one. Then update backend settings.

    Args:
        data (dict): validated data of the new storage backend settings
    """
    domain = get_domain()
    old_storage = domain.get_storage()
    new_storage = DomainBackendMigratorSerializer(data=data).create_storage()

    artifacts = Artifact.objects.filter(pulp_domain=domain)
    date = now()

    skipped = []
    with ProgressReport(
        message=_("Migrating Artifacts"), code="migrate", total=artifacts.count()
    ) as pb:
        while True:
            for artifact in pb.iter(artifacts.iterator()):
                old_name = artifact.file.name
                new_name = storage.get_artifact_path(artifact.sha256)
                if not new_storage.exists(new_name):
                    try:
                        file = old_storage.open(old_name)
                    except Exception as e:
                        _logger.warning(
                            "Skipping artifact(sha256=%s): unable to read from old storage: %s",
                            artifact.sha256,
                            e,
                        )
                        skipped.append(artifact.sha256)
                        continue
                    new_storage.save(new_name, file)
                    file.close()
                if old_name != new_name:
                    artifact.file.name = new_name
                    artifact.save(update_fields=["file"])
            # Handle new artifacts saved by the content app
            artifacts = Artifact.objects.filter(pulp_domain=domain, pulp_created__gte=date)
            if count := artifacts.count():
                pb.total += count
                pb.save()
                date = now()
                continue
            break

    if skipped:
        _logger.warning(
            "Migration completed with %d skipped artifacts (missing from old storage): %s",
            len(skipped),
            ", ".join(skipped),
        )

    # Update the current domain to the new storage backend settings
    msg = _("Update Domain({domain})'s Backend Settings").format(domain=domain.name)
    with ProgressReport(message=msg, code="update", total=1) as pb:
        domain.storage_class = data["storage_class"]
        domain.storage_settings = data["storage_settings"]
        domain.save(update_fields=["storage_class", "storage_settings"], skip_hooks=True)
        pb.increment()
