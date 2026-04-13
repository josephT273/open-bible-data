import { Database } from 'bun:sqlite';
import fs from 'node:fs';
import path from 'node:path';

function readJSON<T>(filePath: string): T | null {
    try {
        const raw = fs.readFileSync(filePath, "utf-8");
        return JSON.parse(raw)
    } catch (error) {
        console.log(`Skipped invalid JSON: ${filePath}`);
        return null
    }
}

// manifest
type Manifest = {
    versions: {
        id: string,
        name: string,
        description: string,
        language: string,
        file: string,
        cover_image: string,
    }[]
}

export function readDirSafe(p: string) {
    try {
        return fs.readdirSync(p, "utf8");
    } catch {
        return [];
    }
}

const ROOT = "./bibles";

const EXCLUDED = new Set([
    "Extras",
    "readme.txt",
    "AM-Amharic",
    "Bible-kjv",
    "amharic_bible",
]);


const active_bibles = async () => {
    const location = "./bibles/active"
    const metadataFile = `${location}/version_manifest.json`;
    const dir = fs.readdirSync(location, "utf-8");

    const data: Manifest = readJSON(metadataFile)!;

    for (const info of data.versions) {
        const db = new Database(
            `./sqlite/${info.name.replace(/\s+/g, "_")}.sqlite`
        );

        db.exec("PRAGMA foreign_keys = ON;");

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

                CREATE TABLE IF NOT EXISTS book_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metadataID INTEGER NOT NULL,
                    book_name TEXT NOT NULL,
                    total_chapter TEXT NOT NULL,
                    FOREIGN KEY (metadataID) REFERENCES metadata(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS verses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    book INTEGER NOT NULL,
                    chapter TEXT NOT NULL,
                    verse TEXT NOT NULL,
                    text TEXT NOT NULL,
                    title TEXT,
                    FOREIGN KEY (book) REFERENCES book_info(id) ON DELETE CASCADE
                );
                `);

        const metadataId = db
            .prepare(
                `INSERT INTO metadata (name, shortname, description, lang)
         VALUES (?, ?, ?, ?)`
            )
            .run(info.name, info.id, info.description, info.language)
            .lastInsertRowid;

        const bible = readJSON<any>(path.join(location, info.file));
        if (!bible) continue;

        const insertBook = db.prepare(`
            INSERT INTO book_info (metadataID, book_name, total_chapter)
            VALUES (?, ?, ?)
            `);
        const insertVerse = db.prepare(`
            INSERT INTO verses (book, chapter, verse, text, title)
            VALUES (?, ?, ?, ?, ?)
            `);
        const insertAll = db.transaction(() => {
            for (const bookName in bible) {
                const chapters = bible[bookName];

                // insert book
                const bookId = insertBook
                    .run(metadataId, bookName, Object.keys(chapters).length.toString())
                    .lastInsertRowid;

                for (const chapterNum in chapters) {
                    const verses = chapters[chapterNum];

                    for (const verseNum in verses) {
                        const value = verses[verseNum];

                        let text: string;
                        let title: string | null = null;

                        if (typeof value === "string") {
                            text = value;
                        } else {
                            text = value.text;
                            title = value.title ?? null;
                        }

                        insertVerse.run(
                            bookId,
                            chapterNum,
                            verseNum,
                            text,
                            title
                        );
                    }
                }
            }
        });

        insertAll();

        console.log(`✅ Imported: ${info.name}`);
    }
}

const OUTPUT = "./sqlite";

export function migration() {
    fs.mkdirSync(OUTPUT, { recursive: true });

    for (const folder of fs.readdirSync(ROOT)) {
        if (EXCLUDED.has(folder)) {
            console.log(`⏭️ Skipped: ${folder}`);
            continue;
        }

        const folderPath = path.join(ROOT, folder);
        if (!fs.statSync(folderPath).isDirectory()) continue;

        console.log(`\n🌍 Processing: ${folder}`);

        const db = new Database(`${OUTPUT}/${folder}.sqlite`);
        db.exec("PRAGMA foreign_keys = ON;");

        // =========================
        // TABLES
        // =========================
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
        book_id INTEGER NOT NULL,
        chapter INTEGER NOT NULL,
        verse INTEGER NOT NULL,
        text TEXT NOT NULL,
        title TEXT,
        FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_lookup
      ON verses(book_id, chapter, verse);
    `);

        // =========================
        // PREPARED STATEMENTS
        // =========================
        const insertMeta = db.prepare(`
      INSERT INTO metadata (
        name, shortname, module, year, publisher, owner,
        description, lang, lang_short, copyright,
        copyright_statement, url, citation_limit,
        restrict, italics, strongs, red_letter,
        paragraph, official, research, module_version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

        const insertBook = db.prepare(`
      INSERT OR IGNORE INTO books (name, total_chapter)
      VALUES (?, ?)
    `);

        const getBookId = db.prepare(`
      SELECT id FROM books WHERE name = ?
    `);

        const insertVerse = db.prepare(`
      INSERT INTO verses (book_id, chapter, verse, text, title)
      VALUES (?, ?, ?, ?, ?)
    `);

        // =========================
        // MIGRATION TRANSACTION
        // =========================
        const insertAll = db.transaction(() => {
            const files = fs.readdirSync(folderPath);

            for (const file of files) {
                if (!file.endsWith(".json")) continue;

                const data = readJSON<any>(path.join(folderPath, file));
                if (!data) continue;

                // =========================
                // METADATA (STORE EVERYTHING)
                // =========================
                if (data.metadata) {
                    insertMeta.run(
                        data.metadata.name ?? "",
                        data.metadata.shortname ?? "",
                        data.metadata.module ?? "",
                        String(data.metadata.year ?? ""),
                        data.metadata.publisher ?? null,
                        data.metadata.owner ?? null,
                        data.metadata.description ?? null,
                        data.metadata.lang ?? null,
                        data.metadata.lang_short ?? null,
                        String(data.metadata.copyright ?? ""),
                        data.metadata.copyright_statement ?? null,
                        data.metadata.url ?? null,
                        String(data.metadata.citation_limit ?? ""),
                        String(data.metadata.restrict ?? ""),
                        String(data.metadata.italics ?? ""),
                        String(data.metadata.strongs ?? ""),
                        String(data.metadata.red_letter ?? ""),
                        String(data.metadata.paragraph ?? ""),
                        String(data.metadata.official ?? ""),
                        String(data.metadata.research ?? ""),
                        data.metadata.module_version ?? ""
                    );
                }

                // =========================
                // BOOK + VERSES
                // =========================
                if (Array.isArray(data.verses)) {
                    const bookChapterMap = new Map<string, number>();

                    // PASS 1: compute total chapters
                    for (const v of data.verses) {
                        const bookName = v.book_name?.trim();
                        if (!bookName) continue;

                        const chapter = Number(v.chapter ?? 0);
                        bookChapterMap.set(
                            bookName,
                            Math.max(bookChapterMap.get(bookName) ?? 0, chapter)
                        );
                    }

                    // PASS 2: insert books
                    for (const [bookName, totalChapter] of bookChapterMap) {
                        insertBook.run(bookName, totalChapter);
                    }

                    // PASS 3: insert verses
                    for (const v of data.verses) {
                        const bookName = v.book_name?.trim();
                        if (!bookName) continue;

                        const bookId = getBookId.get(bookName)?.id;
                        if (!bookId) continue;

                        insertVerse.run(
                            bookId,
                            Number(v.chapter ?? 0),
                            Number(v.verse ?? 0),
                            v.text ?? "",
                            v.title ?? null
                        );
                    }
                }
            }
        });

        insertAll();

        console.log(`✅ Done: ${folder}`);
    }

    console.log("\n🎉 Migration completed!");
}

active_bibles()
migration()