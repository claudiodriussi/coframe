class Archivable:
    """
    Mixin that adds soft-delete (archive) behaviour to a table.

    Protocol attributes read by the querybuilder to apply automatic filtering:
      _cf_archive_field  — column name that controls visibility (default: 'active')
      _cf_archive_value  — value that means "not archived" (default: True)

    Supports different field conventions:
      active=True   → visible   (Archivable default)
      archived=True → hidden    → set _cf_archive_value = False
      visible=True  → visible   → set field name to 'visible'
    """
    _cf_archive_field: str = 'active'
    _cf_archive_value: bool = True

    def archive(self) -> None:
        """Mark record as archived (hidden from default queries)."""
        setattr(self, self._cf_archive_field, not self._cf_archive_value)

    def unarchive(self) -> None:
        """Restore record to active state."""
        setattr(self, self._cf_archive_field, self._cf_archive_value)

    @property
    def is_archived(self) -> bool:
        """True if the record is currently archived."""
        return getattr(self, self._cf_archive_field) != self._cf_archive_value

    # ── Query behavior protocol ────────────────────────────────────────────
    # Registered via app.add_query_behavior(Archivable) at startup.

    @classmethod
    def applies_to(cls, model_class) -> bool:
        """True if model_class carries the archive protocol."""
        return hasattr(model_class, '_cf_archive_field')

    @classmethod
    def apply(cls, model_class, query_def: dict, query):
        """Add implicit WHERE active=True unless caller opts out."""
        field = getattr(model_class, '_cf_archive_field')
        value = getattr(model_class, '_cf_archive_value', True)
        if query_def.get('include_archived'):
            return query
        if field in query_def.get('filters', {}):
            return query  # caller filters explicitly — don't interfere
        col = getattr(model_class, field, None)
        if col is not None:
            query = query.where(col == value)
        return query
