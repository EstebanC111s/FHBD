-- ============================================================
-- Consultas Trino — Replicar tablas Gold desde Silver
-- Ejecutar en Trino (http://localhost:8085)
-- Catálogo: iceberg  |  Esquemas: silver, gold
-- ============================================================

-- ── 1. Verificar tablas Silver disponibles ──
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;

-- ── 2. Consultar datos Silver ──
SELECT COUNT(*) AS total_users FROM iceberg.silver.users_hist;
SELECT COUNT(*) AS total_posts FROM iceberg.silver.posts_hist;

-- Muestra de users_hist
SELECT * FROM iceberg.silver.users_hist LIMIT 10;

-- Muestra de posts_hist
SELECT * FROM iceberg.silver.posts_hist LIMIT 10;

-- ── 3. Replicar Gold: post_counts_by_user (desde Silver) ──
-- Esta query replica la lógica de la tabla Gold post_counts_by_user
-- usando únicamente las tablas de Silver.

SELECT
    p.OwnerUserId,
    COUNT(*) AS total_posts,
    SUM(CASE WHEN p.PostTypeId = '1' THEN 1 ELSE 0 END) AS total_preguntas,
    SUM(CASE WHEN p.PostTypeId = '2' THEN 1 ELSE 0 END) AS total_respuestas,
    SUM(COALESCE(CAST(p.Score AS INTEGER), 0)) AS total_score,
    SUM(COALESCE(CAST(p.ViewCount AS INTEGER), 0)) AS total_views,
    SUM(COALESCE(CAST(p.CommentCount AS INTEGER), 0)) AS total_comments,
    SUM(COALESCE(CAST(p.AnswerCount AS INTEGER), 0)) AS total_answers_received,
    u.DisplayName,
    u.Reputation,
    u.Location,
    u.UpVotes,
    u.DownVotes,
    u.Views AS user_views,
    CURRENT_TIMESTAMP AS fecha_cargue
FROM iceberg.silver.posts_hist p
LEFT JOIN iceberg.silver.users_hist u
    ON p.OwnerUserId = CAST(u.Id AS VARCHAR)
GROUP BY
    p.OwnerUserId,
    u.DisplayName,
    u.Reputation,
    u.Location,
    u.UpVotes,
    u.DownVotes,
    u.Views
ORDER BY total_posts DESC
LIMIT 20;
