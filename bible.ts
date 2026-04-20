import { Database } from "bun:sqlite";
import fs from "node:fs";
import path from "node:path";

const ROOT = "./bibles";
const OUTPUT = "./sqlite";
const EXCLUDED = new Set(["Extras", "readme.txt", "AM-Amharic", "Bible-kjv", "amharic_bible", "active"]);

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

function readJSON<T>(filePath: string): T | null {
    try {
        return JSON.parse(fs.readFileSync(filePath, "utf-8"));
    } catch {
        console.warn(`Skipped invalid JSON: ${filePath}`);
        return null;
    }
}

function readDirSafe(dir: string): string[] {
    try {
        return fs.readdirSync(dir);
    } catch {
        return [];
    }
}

function createFreshDatabase(dbPath: string) {
    fs.mkdirSync(OUTPUT, { recursive: true });
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    const journalPath = `${dbPath}-journal`;
    if (fs.existsSync(journalPath)) fs.unlinkSync(journalPath);
    return new Database(dbPath);
}

function configureForInsert(db: Database) {
    db.exec(`
    PRAGMA page_size = 4096;
    PRAGMA journal_mode = OFF;
    PRAGMA synchronous = OFF;
    PRAGMA locking_mode = EXCLUSIVE;
    PRAGMA foreign_keys = ON;
    PRAGMA temp_store = MEMORY;
    PRAGMA cache_size = -8000;
    PRAGMA auto_vacuum = NONE;
  `);
}

function createSchema(db: Database) {
    db.exec(`
    CREATE TABLE IF NOT EXISTS metadata (
      id INTEGER PRIMARY KEY NOT NULL,
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
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE,
      total_chapter INTEGER
    );

    CREATE TABLE IF NOT EXISTS verses (
      id INTEGER PRIMARY KEY,
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

function finalizeDatabase(db: Database) {
    db.exec(`
    PRAGMA optimize;
    VACUUM;
  `);
}

type AnyObj = Record<string, any>;
type MetadataFallback = {
    name: string;
    shortname: string;
    lang: string;
    lang_short: string;
    description?: string;
};

type DbContext = {
    db: Database;
    insertMetaStmt: any;
    insertBookStmt: any;
    getBookIdStmt: any;
    insertVerseStmt: any;
};

function createDbContext(dbPath: string): DbContext {
    const db = createFreshDatabase(dbPath);
    configureForInsert(db);
    createSchema(db);

    return {
        db,
        insertMetaStmt: db.prepare(`
      INSERT INTO metadata (
        name, shortname, module, year, publisher, owner,
        description, lang, lang_short, copyright,
        copyright_statement, url, citation_limit,
        restrict, italics, strongs, red_letter,
        paragraph, official, research, module_version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
        insertBookStmt: db.prepare(`
      INSERT OR IGNORE INTO books (name, total_chapter)
      VALUES (?, ?)
    `),
        getBookIdStmt: db.prepare(`SELECT id FROM books WHERE name = ?`),
        insertVerseStmt: db.prepare(`
      INSERT INTO verses (metadataID, book_id, chapter, verse, text, title)
      VALUES (?, ?, ?, ?, ?, ?)
    `),
    };
}

function insertMetadata(stmt: any, metadata: AnyObj | undefined, fallback: MetadataFallback): number {
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

function parseVerseValue(value: any): { text: string; title: string | null } {
    if (typeof value === "string") return { text: value.trim(), title: null };
    if (value && typeof value === "object") {
        return {
            text: String(value.text ?? "").trim(),
            title: value.title == null ? null : String(value.title).trim() || null,
        };
    }
    return { text: "", title: null };
}

function toNumberOrZero(value: unknown): number {
    const direct = Number(value);
    if (Number.isFinite(direct)) return direct;
    const match = String(value ?? "").match(/\d+/);
    return match ? Number(match[0]) : 0;
}

function importNestedBible(data: AnyObj, metadataId: number, ctx: DbContext) {
    for (const [bookRaw, chapters] of Object.entries(data)) {
        if (bookRaw === "metadata" || !chapters || typeof chapters !== "object") continue;
        const bookName = bookRaw.trim();
        if (!bookName) continue;

        const totalChapters = Object.keys(chapters as AnyObj).length;
        ctx.insertBookStmt.run(bookName, totalChapters);

        const row = ctx.getBookIdStmt.get(bookName) as { id: number } | undefined;
        if (!row) continue;
        for (const [chapterRaw, verses] of Object.entries(chapters as AnyObj)) {
            if (!verses || typeof verses !== "object") continue;
            const chapter = toNumberOrZero(chapterRaw);

            for (const [verseRaw, value] of Object.entries(verses as AnyObj)) {
                const verse = toNumberOrZero(verseRaw);
                const parsed = parseVerseValue(value);
                ctx.insertVerseStmt.run(metadataId, row.id, chapter, verse, parsed.text, parsed.title);
            }
        }
    }
}

function importChaptersBible(data: AnyObj, metadataId: number, ctx: DbContext) {
    const bookName = String(data.book ?? data.title ?? "").trim();
    if (!bookName || !Array.isArray(data.chapters)) return;

    ctx.insertBookStmt.run(bookName, data.chapters.length);
    const row = ctx.getBookIdStmt.get(bookName) as { id: number } | undefined;
    if (!row) return;

    for (const chapterObj of data.chapters) {
        const chapter = toNumberOrZero(chapterObj?.chapter ?? 0);
        const verses = Array.isArray(chapterObj?.verses) ? chapterObj.verses : [];

        for (const verseObj of verses) {
            const verse = toNumberOrZero(verseObj?.verse ?? 0);
            const text = String(verseObj?.text ?? "").trim();
            const title = verseObj?.title == null ? null : String(verseObj.title).trim() || null;
            ctx.insertVerseStmt.run(metadataId, row.id, chapter, verse, text, title);
        }
    }
}

function importFlatBible(data: AnyObj, metadataId: number, ctx: DbContext) {
    const verses = Array.isArray(data.verses) ? data.verses : [];
    const maxChapterByBook = new Map<string, number>();
    const bookIdCache = new Map<string, number>();

    for (const v of verses) {
        const bookName = String(v?.book_name ?? "").trim();
        if (!bookName) continue;
        const chapter = toNumberOrZero(v?.chapter ?? 0);
        maxChapterByBook.set(bookName, Math.max(maxChapterByBook.get(bookName) ?? 0, chapter));
    }

    for (const [bookName, totalChapter] of maxChapterByBook) {
        ctx.insertBookStmt.run(bookName, totalChapter);
        const row = ctx.getBookIdStmt.get(bookName) as { id: number } | undefined;
        if (row) bookIdCache.set(bookName, row.id);
    }

    for (const v of verses) {
        const bookName = String(v?.book_name ?? "").trim();
        const bookId = bookIdCache.get(bookName);
        if (!bookId) continue;

        const text = String(v?.text ?? "").trim();
        const title = v?.title == null ? null : String(v.title).trim() || null;

        ctx.insertVerseStmt.run(
            metadataId,
            bookId,
            toNumberOrZero(v?.chapter ?? 0),
            toNumberOrZero(v?.verse ?? 0),
            text,
            title
        );
    }
}

function importBibleData(data: AnyObj, metadataId: number, ctx: DbContext) {
    if (Array.isArray(data.chapters)) {
        importChaptersBible(data, metadataId, ctx);
        return;
    }
    if (Array.isArray(data.verses)) {
        importFlatBible(data, metadataId, ctx);
        return;
    }
    importNestedBible(data, metadataId, ctx);
}

function safeDbName(name: string): string {
    return name.replace(/[\\/:*?"<>|]+/g, "_").replace(/\s+/g, "_");
}

function isSplitBookFolder(files: string[], folderPath: string): boolean {
    for (const file of files) {
        const data = readJSON<any>(path.join(folderPath, file));
        if (!data || Array.isArray(data)) continue;
        return Array.isArray(data.chapters);
    }
    return false;
}

export function active_bibles() {
    const location = path.join(ROOT, "active");
    const manifestPath = path.join(location, "version_manifest.json");

    const manifest = readJSON<Manifest>(manifestPath);
    if (!manifest) {
        console.error("Invalid or missing version_manifest.json");
        return;
    }

    for (const info of manifest.versions) {
        console.log(`Active: ${info.name}`);

        const ctx = createDbContext(path.join(OUTPUT, `${safeDbName(info.id)}.sqlite`));

        const metadataId = insertMetadata(ctx.insertMetaStmt, undefined, {
            name: info.name,
            shortname: info.id,
            lang: info.language,
            lang_short: (info.language ?? "").slice(0, 2).toUpperCase(),
            description: info.description,
        });

        const bible = readJSON<any>(path.join(location, info.file));
        if (!bible) {
            console.warn(`Skipped translation file: ${info.file}`);
            ctx.db.close();
            continue;
        }

        const insertAll = ctx.db.transaction(() => importBibleData(bible, metadataId, ctx));
        insertAll();
        finalizeDatabase(ctx.db);
        ctx.db.close();

        console.log(`Done: ${info.name}`);
    }
}

export function migration() {
    for (const folder of readDirSafe(ROOT)) {
        if (EXCLUDED.has(folder)) {
            continue;
        }

        const folderPath = path.join(ROOT, folder);
        if (!fs.statSync(folderPath).isDirectory()) continue;

        console.log(`Language: ${folder}`);

        const files = readDirSafe(folderPath).filter((f) => f.endsWith(".json"));
        if (!files.length) continue;

        if (isSplitBookFolder(files, folderPath)) {
            const ctx = createDbContext(path.join(OUTPUT, `${safeDbName(folder)}.sqlite`));
            const metadataId = insertMetadata(ctx.insertMetaStmt, undefined, {
                name: folder,
                shortname: folder,
                lang: folder,
                lang_short: folder.split("-")[0] ?? "",
            });

            const insertAll = ctx.db.transaction(() => {
                for (const file of files) {
                    const data = readJSON<any>(path.join(folderPath, file));
                    if (!data || Array.isArray(data)) continue;
                    importBibleData(data, metadataId, ctx);
                }
            });

            insertAll();
            finalizeDatabase(ctx.db);
            ctx.db.close();
        } else {
            for (const file of files) {
                const data = readJSON<any>(path.join(folderPath, file));
                if (!data || Array.isArray(data)) continue;

                const outputFile = `${safeDbName(folder)}_${safeDbName(path.parse(file).name)}.sqlite`;
                const ctx = createDbContext(path.join(OUTPUT, outputFile));

                const metadataId = insertMetadata(ctx.insertMetaStmt, data.metadata, {
                    name: path.parse(file).name,
                    shortname: path.parse(file).name,
                    lang: folder,
                    lang_short: folder.split("-")[0] ?? "",
                });

                const insertAll = ctx.db.transaction(() => importBibleData(data, metadataId, ctx));
                insertAll();
                finalizeDatabase(ctx.db);
                ctx.db.close();
            }
        }

        console.log(`Done: ${folder}`);
    }

    console.log("Migration completed");
}
active_bibles();
migration();
