import { Database } from "bun:sqlite";
import fs from "node:fs";
import path from "node:path";

/* ============================================================
 * CONFIGURATION
 * ============================================================ */

const ROOT = "./bibles";
const OUTPUT = "./sqlite";

/**
 * Folders excluded from language-based migration.
 * The "active" folder is processed separately.
 */
const EXCLUDED = new Set([
    "Extras",
    "readme.txt",
    "AM-Amharic",
    "Bible-kjv",
    "amharic_bible",
    "active",
]);

/* ============================================================
 * TYPES
 * ============================================================ */

type Manifest = {
    versions: {
        id: string;
        name: string;
        description: string;
        language: string;
        file: string;
        cover_image: string;
    }[];
};

/* ============================================================
 * UTILITY FUNCTIONS
 * ============================================================ */

/**
 * Safely parse JSON files.
 */
function readJSON<T>(filePath: string): T | null {
    try {
        const raw = fs.readFileSync(filePath, "utf-8");
        return JSON.parse(raw);
    } catch {
        console.warn(`⚠️  Skipped invalid JSON: ${filePath}`);
        return null;
    }
}

/**
 * Safely read directory contents.
 */
function readDirSafe(dir: string): string[] {
    try {
        return fs.readdirSync(dir);
    } catch {
        return [];
    }
}

/**
 * Apply SQLite performance optimizations.
 */
function applyPragmas(db: Database) {
    db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA foreign_keys = ON;
    PRAGMA temp_store = MEMORY;
    PRAGMA cache_size = -64000;
  `);
}

/**
 * Create unified database schema.
 */
function createSchema(db: Database) {
    db.exec(`
    CREATE TABLE IF NOT EXISTS metadata (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name TEXT NOT NULL,
      shortname TEXT,
      module TEXT,
      year TEXT,
      publisher TEXT,
      owner TEXT,
      description TEXT,
      lang TEXT,
      lang_short TEXT,
      copyright TEXT,
      copyright_statement TEXT,
      url TEXT,
      citation_limit TEXT,
      restrict TEXT,
      italics TEXT,
      strongs TEXT,
      red_letter TEXT,
      paragraph TEXT,
      official TEXT,
      research TEXT,
      module_version TEXT
    );

    CREATE TABLE IF NOT EXISTS books (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE,
      total_chapter INTEGER
    );

    CREATE TABLE IF NOT EXISTS verses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      metadataID INTEGER NOT NULL,
      book_id INTEGER NOT NULL,
      chapter INTEGER NOT NULL,
      verse INTEGER NOT NULL,
      text TEXT NOT NULL,
      title TEXT,
      FOREIGN KEY (metadataID) REFERENCES metadata(id) ON DELETE CASCADE,
      FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_lookup
    ON verses(metadataID, book_id, chapter, verse);
  `);
}

/**
 * Insert metadata with fallback values while preserving all fields.
 */
function insertFullMetadata(
    stmt: any,
    metadata: any,
    fallback: {
        name: string;
        shortname: string;
        lang: string;
        lang_short: string;
        description?: string;
    }
): number {
    return stmt.run(
        metadata?.name ?? fallback.name,
        metadata?.shortname ?? fallback.shortname,
        metadata?.module ?? null,
        metadata?.year ?? null,
        metadata?.publisher ?? null,
        metadata?.owner ?? null,
        metadata?.description ?? fallback.description ?? null,
        metadata?.lang ?? fallback.lang,
        metadata?.lang_short ?? fallback.lang_short,
        metadata?.copyright ?? null,
        metadata?.copyright_statement ?? null,
        metadata?.url ?? null,
        metadata?.citation_limit ?? null,
        metadata?.restrict ?? null,
        metadata?.italics ?? null,
        metadata?.strongs ?? null,
        metadata?.red_letter ?? null,
        metadata?.paragraph ?? null,
        metadata?.official ?? null,
        metadata?.research ?? null,
        metadata?.module_version ?? null
    ).lastInsertRowid as number;
}

/* ============================================================
 * ACTIVE FOLDER MIGRATION
 * ============================================================ */

export function active_bibles() {
    const location = path.join(ROOT, "active");
    const manifestPath = path.join(location, "version_manifest.json");

    const manifest = readJSON<Manifest>(manifestPath);
    if (!manifest) {
        console.error("❌ Invalid or missing version_manifest.json");
        return;
    }

    fs.mkdirSync(OUTPUT, { recursive: true });

    for (const info of manifest.versions) {
        console.log(`\n📖 Importing Active Translation: ${info.name}`);

        const dbPath = path.join(
            OUTPUT,
            `${info.name.replace(/\s+/g, "_")}.sqlite`
        );
        const db = new Database(dbPath);

        applyPragmas(db);
        createSchema(db);

        const insertMetaStmt = db.prepare(`
      INSERT INTO metadata (
        name, shortname, module, year, publisher, owner,
        description, lang, lang_short, copyright,
        copyright_statement, url, citation_limit,
        restrict, italics, strongs, red_letter,
        paragraph, official, research, module_version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

        const insertBookStmt = db.prepare(`
      INSERT OR IGNORE INTO books (name, total_chapter)
      VALUES (?, ?)
    `);

        const getBookIdStmt = db.prepare(
            `SELECT id FROM books WHERE name = ?`
        );

        const insertVerseStmt = db.prepare(`
      INSERT INTO verses (metadataID, book_id, chapter, verse, text, title)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

        const metadataId = insertFullMetadata(
            insertMetaStmt,
            {}, // Manifest lacks full metadata
            {
                name: info.name,
                shortname: info.id,
                lang: info.language,
                lang_short: info.language?.substring(0, 2).toUpperCase(),
                description: info.description,
            }
        );

        const bible = readJSON<any>(path.join(location, info.file));
        if (!bible) {
            console.warn(`⚠️  Skipped translation file: ${info.file}`);
            db.close();
            continue;
        }

        const insertAll = db.transaction(() => {
            for (const bookNameRaw in bible) {
                if (bookNameRaw === "metadata") continue;

                const bookName = bookNameRaw.trim();
                const chapters = bible[bookNameRaw];
                const totalChapters = Object.keys(chapters).length;

                insertBookStmt.run(bookName, totalChapters);
                const bookRow = getBookIdStmt.get(bookName);
                if (!bookRow) continue;

                const bookId = bookRow.id;

                for (const chapterNum in chapters) {
                    const verses = chapters[chapterNum];

                    for (const verseNum in verses) {
                        const value = verses[verseNum];

                        let text: string;
                        let title: string | null = null;

                        // Handle both string and object verse formats
                        if (typeof value === "string") {
                            text = value;
                        } else if (value && typeof value === "object") {
                            text = value.text ?? "";
                            title = value.title ?? null;
                        } else {
                            text = "";
                        }

                        insertVerseStmt.run(
                            metadataId,
                            bookId,
                            Number(chapterNum) || 0,
                            Number(verseNum) || 0,
                            text,
                            title
                        );
                    }
                }
            }
        });

        insertAll();
        db.close();

        console.log(`✅ Completed: ${info.name}`);
    }
}

/* ============================================================
 * LANGUAGE FOLDER MIGRATION
 * ============================================================ */

export function migration() {
    fs.mkdirSync(OUTPUT, { recursive: true });

    for (const folder of readDirSafe(ROOT)) {
        if (EXCLUDED.has(folder)) {
            console.log(`⏭️ Skipped: ${folder}`);
            continue;
        }

        const folderPath = path.join(ROOT, folder);
        if (!fs.statSync(folderPath).isDirectory()) continue;

        console.log(`\n🌍 Processing Language: ${folder}`);

        const db = new Database(path.join(OUTPUT, `${folder}.sqlite`));
        applyPragmas(db);
        createSchema(db);

        const insertMetaStmt = db.prepare(`
      INSERT INTO metadata (
        name, shortname, module, year, publisher, owner,
        description, lang, lang_short, copyright,
        copyright_statement, url, citation_limit,
        restrict, italics, strongs, red_letter,
        paragraph, official, research, module_version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

        const insertBookStmt = db.prepare(`
      INSERT OR IGNORE INTO books (name, total_chapter)
      VALUES (?, ?)
    `);

        const getBookIdStmt = db.prepare(
            `SELECT id FROM books WHERE name = ?`
        );

        const insertVerseStmt = db.prepare(`
      INSERT INTO verses (metadataID, book_id, chapter, verse, text, title)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

        const insertAll = db.transaction(() => {
            const files = readDirSafe(folderPath).filter((f) =>
                f.endsWith(".json")
            );

            for (const file of files) {
                console.log(`   📘 Importing: ${file}`);

                const filePath = path.join(folderPath, file);
                const data = readJSON<any>(filePath);
                if (!data) continue;

                const metadataId = insertFullMetadata(
                    insertMetaStmt,
                    data.metadata,
                    {
                        name: path.parse(file).name,
                        shortname: path.parse(file).name,
                        lang: folder,
                        lang_short: folder.split("-")[0]!,
                    }
                );

                const bookChapterMap = new Map<string, number>();
                const bookIdCache = new Map<string, number>();

                // ----------- FLAT FORMAT (verses array) -----------
                if (Array.isArray(data.verses)) {
                    for (const v of data.verses) {
                        const bookName = (v.book_name ?? "").trim();
                        if (!bookName) continue;

                        const chapter = Number(v.chapter ?? 0);
                        bookChapterMap.set(
                            bookName,
                            Math.max(bookChapterMap.get(bookName) ?? 0, chapter)
                        );
                    }

                    for (const [bookName, totalChapter] of bookChapterMap) {
                        insertBookStmt.run(bookName, totalChapter);
                        const row = getBookIdStmt.get(bookName);
                        if (row) bookIdCache.set(bookName, row.id);
                    }

                    for (const v of data.verses) {
                        const bookName = (v.book_name ?? "").trim();
                        const bookId = bookIdCache.get(bookName);
                        if (!bookId) continue;

                        insertVerseStmt.run(
                            metadataId,
                            bookId,
                            Number(v.chapter ?? 0),
                            Number(v.verse ?? 0),
                            v.text ?? "",
                            v.title ?? null
                        );
                    }
                }
                // ----------- NESTED FORMAT -----------
                else {
                    for (const bookNameRaw in data) {
                        if (bookNameRaw === "metadata") continue;

                        const bookName = bookNameRaw.trim();
                        const chapters = data[bookNameRaw];
                        const totalChapters = Object.keys(chapters).length;

                        insertBookStmt.run(bookName, totalChapters);
                        const row = getBookIdStmt.get(bookName);
                        if (!row) continue;

                        const bookId = row.id;

                        for (const chapterNum in chapters) {
                            const verses = chapters[chapterNum];

                            for (const verseNum in verses) {
                                const value = verses[verseNum];

                                let text: string;
                                let title: string | null = null;

                                if (typeof value === "string") {
                                    text = value;
                                } else if (value && typeof value === "object") {
                                    text = value.text ?? "";
                                    title = value.title ?? null;
                                } else {
                                    text = "";
                                }

                                insertVerseStmt.run(
                                    metadataId,
                                    bookId,
                                    Number(chapterNum) || 0,
                                    Number(verseNum) || 0,
                                    text,
                                    title
                                );
                            }
                        }
                    }
                }
            }
        });

        insertAll();
        db.close();

        console.log(`✅ Completed Language: ${folder}`);
    }

    console.log("\n🎉 Migration completed successfully!");
}
active_bibles(); // Process manifest-based translations
migration();     // Process language-based folders