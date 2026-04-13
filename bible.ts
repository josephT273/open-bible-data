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
active_bibles()