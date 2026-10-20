-- The one row that names this database and records which Taskline version last migrated it.
-- taskline migrate writes it; nothing else does.
CREATE TABLE jobs.instance (
  instance_id      BOOLEAN PRIMARY KEY DEFAULT true CHECK (instance_id),  -- only one row can exist
  name             TEXT NOT NULL,
  taskline_version TEXT NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
