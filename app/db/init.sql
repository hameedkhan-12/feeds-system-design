-- ─────────────────────────────────────────────
--  Schema for feeddb
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    username   TEXT   NOT NULL UNIQUE,
    email      TEXT   NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS posts (
    id         SERIAL PRIMARY KEY,
    user_id    INT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content    TEXT   NOT NULL,
    likes      INT    NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_posts_user_id    ON posts (user_id);
CREATE INDEX IF NOT EXISTS idx_posts_likes_desc ON posts (likes DESC);
CREATE INDEX IF NOT EXISTS idx_posts_created    ON posts (created_at DESC);

-- ─────────────────────────────────────────────
--  Seed data
-- ─────────────────────────────────────────────
INSERT INTO users (username, email) VALUES
    ('alice',   'alice@example.com'),
    ('bob',     'bob@example.com'),
    ('carol',   'carol@example.com'),
    ('dave',    'dave@example.com')
ON CONFLICT DO NOTHING;

INSERT INTO posts (user_id, content, likes) VALUES
    (1, 'Hello from Alice! This is my first post.',  42),
    (1, 'Alice shares another update today.',         15),
    (2, 'Bob here — working on distributed systems.', 99),
    (2, 'Bob loves Redis and PostgreSQL.',            77),
    (3, 'Carol is exploring sharding strategies.',    33),
    (4, 'Dave is benchmarking cache vs DB latency.',  50)
ON CONFLICT DO NOTHING;