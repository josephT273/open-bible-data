import { int, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const metadata = sqliteTable("metadata", {
    id: int().primaryKey({ autoIncrement: true }).notNull(),
    name: text().notNull(),
    shortname: text(),
    module: text(),
    year: text(),
    publisher: text(),
    owner: text(),
    description: text(),
    lang: text(),
    lang_short: text(),
    copyright: text(),
    copyright_statement: text(),
    url: text(),
    citation_limit: text(),
    restrict: text(),
    italics: text(),
    strongs: text(),
    red_letter: text(),
    paragraph: text(),
    official: text(),
    research: text(),
    module_version: text(),
})

export const bookInfo = sqliteTable("book_info", {
    id: int().primaryKey({ autoIncrement: true }),
    metadataID: int().notNull().references(() => metadata.id, { onDelete: 'cascade' }),
    book_name: text().notNull(),
    total_chapter: text().notNull(),
})


export const verses = sqliteTable("verses", {
    id: int().primaryKey({ autoIncrement: true }),
    book: int().references(() => bookInfo.id, { onDelete: 'cascade' }).notNull(),
    chapter: text().notNull(),
    verse: text().notNull(),
    text: text().notNull(),
    title: text()
});
