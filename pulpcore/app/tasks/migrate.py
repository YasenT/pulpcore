import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from gettext import gettext as _

from django.conf import settings
from django.utils.timezone import now

from pulpcore.app.models import Artifact, ProgressReport, storage
from pulpcore.app.serializers import DomainBackendMigratorSerializer
from pulpcore.app.util import get_domain

_logger = logging.getLogger(__name__)

MIGRATION_WORKERS = getattr(settings, "MIGRATION_WORKERS", 8)
MIGRATION_BATCH_SIZE = getattr(settings, "MIGRATION_BATCH_SIZE", 500)


def _copy_artifact(old_storage, new_storage, old_name, new_name, sha256):
    """Copy a single artifact between storage backends. Returns (sha256, new_name, error)."""
    if new_storage.exists(new_name):
        return sha256, new_name, old_name != new_name, None
    try:
        file = old_storage.open(old_name)
    except Exception as e:
        return sha256, new_name, False, e
    try:
        new_storage.save(new_name, file)
    finally:
        file.close()
    return sha256, new_name, old_name != new_name, None


def _process_batch(batch, pending_updates, skipped, pb):
    """Wait for a batch of copy futures and collect results."""
    for future in as_completed(batch):
        sha256, new_name, needs_update, error = future.result()
        if error is not None:
            _logger.warning(
                "Skipping artifact(sha256=%s): unable to read from old storage: %s",
                sha256,
                error,
            )
            skipped.append(sha256)
        elif needs_update:
            pending_updates[sha256] = new_name
        pb.increment()


def _flush_db_updates(domain, pending_updates):
    """Batch-update artifact file names in the database."""
    qs = Artifact.objects.filter(pulp_domain=domain, sha256__in=pending_updates.keys())
    to_update = []
    for artifact in qs.iterator():
        new_name = pending_updates.get(artifact.sha256)
        if new_name and artifact.file.name != new_name:
            artifact.file.name = new_name
            to_update.append(artifact)
    if to_update:
        Artifact.objects.bulk_update(to_update, ["file"], batch_size=1000)


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
            batch = []
            pending_updates = {}
            with ThreadPoolExecutor(max_workers=MIGRATION_WORKERS) as executor:
                for artifact in artifacts.iterator():
                    old_name = artifact.file.name
                    new_name = storage.get_artifact_path(artifact.sha256)
                    future = executor.submit(
                        _copy_artifact, old_storage, new_storage,
                        old_name, new_name, artifact.sha256,
                    )
                    batch.append(future)

                    if len(batch) >= MIGRATION_BATCH_SIZE:
                        _process_batch(batch, pending_updates, skipped, pb)
                        _flush_db_updates(domain, pending_updates)
                        pending_updates = {}
                        batch = []

                if batch:
                    _process_batch(batch, pending_updates, skipped, pb)

            if pending_updates:
                _flush_db_updates(domain, pending_updates)

            # Handle new artifacts saved by the content app during migration
            artifacts = Artifact.objects.filter(pulp_domain=domain, pulp_created__gte=date)
            if count := artifacts.count():
                pb.total += count
                pb.save()
                date = now()
                continue
            break

    _logger.info(
        "Migration completed for domain %s: %d skipped.",
        domain.name,
        len(skipped),
    )

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
