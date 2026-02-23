CREATE TABLE IF NOT EXISTS library_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    virtual_library_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    collection_type TEXT NOT NULL,
    preview_server_id INTEGER NOT NULL,
    preview_library_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (preview_server_id) REFERENCES servers (id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS library_group_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_group_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    source_library_id TEXT NOT NULL,
    source_library_name TEXT NOT NULL,
    source_collection_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (library_group_id) REFERENCES library_groups (id) ON DELETE CASCADE,
    FOREIGN KEY (server_id) REFERENCES servers (id) ON DELETE CASCADE,
    UNIQUE(library_group_id, server_id, source_library_id)
);

CREATE INDEX IF NOT EXISTS idx_library_groups_virtual_library_id
    ON library_groups(virtual_library_id);

CREATE INDEX IF NOT EXISTS idx_library_group_sources_group_id
    ON library_group_sources(library_group_id);

CREATE INDEX IF NOT EXISTS idx_library_group_sources_server_library
    ON library_group_sources(server_id, source_library_id);
