-- ENUM for status
CREATE TYPE file_status AS ENUM ('new', 'finished');

-- To add new values in ENUM, use
-- ALTER TYPE file_status ADD VALUE IF NOT EXISTS 'new_value_here'


-- Master table for files
CREATE TABLE public.files
(
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  file_date timestamp without time zone NOT NULL,
  record_date timestamp without time zone NOT NULL,
  original_name text NOT NULL,
  generated_name text NOT NULL,
  mime_type text NOT NULL,
  file_extension text NOT NULL,
  source text NOT NULL,
  file_sha256sum text NOT NULL,
  chunk_size integer NOT NULL,
  file_type_hierarchy text[] NULL,
  file_size integer NOT NULL,
  attributes jsonb NULL,
  status file_status NOT NULL,
  CONSTRAINT pk_files PRIMARY KEY (id),
  CONSTRAINT unique_sha256sum_files UNIQUE (file_sha256sum)
)
WITH (
  OIDS=FALSE
);

-- Child table for file data
CREATE TABLE public.file_chunks
(
  id uuid NOT NULL DEFAULT uuid_generate_v4(),
  file_id uuid NOT NULL,
  chunk_number integer NOT NULL,
  chunk_sha256sum text NOT NULL,
  data bytea NOT NULL,
  CONSTRAINT pk_file_chunks PRIMARY KEY (id),
  CONSTRAINT file_chunks_files_id_fkey FOREIGN KEY (file_id)
      REFERENCES public.files (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
