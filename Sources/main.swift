// The Swift Programming Language
// https://docs.swift.org/swift-book

import Foundation
import SQLite
import struct SQLite.Expression // Foundation has its own Expression class now

enum Errs: Error {
    case noSuchItem
    case noContentsError
    case wtf
}

public struct Decloud {

    let db: Connection

    let items = Table("client_items")
    let rowid = Expression<Int>("rowid")
    let item_type = Expression<Int>("item_type")
    let filename = Expression<String>("item_filename")
    let item_blob_id = Expression<SQLite.Blob>("item_id")
    let item_parent_id = Expression<SQLite.Blob>("item_parent_id")
    var paths: [Int: String] = [:]
    var sources: [Int: String] = [:]

    public init () throws {
        let db = try Connection("/path/to/client.db", readonly: true)
//        db.trace { print($0) }
        var sources: [Int: String] = [:]
        let startingPath = "/path/to/session/i"

        guard let contents = try? FileManager.default.contentsOfDirectory(atPath: startingPath) else { throw Errs.noContentsError }
        let regex = /^f([0-9a-f]*)\..*$/

        try contents.forEach { item in
            if let matches = try? regex.wholeMatch(in: item) {
                let code = (matches.1)
                guard let intCode = Int(code, radix: 16) else { throw Errs.wtf }

                sources[intCode] = startingPath + "/" + item
            }
        }

        self.sources = sources
        self.db = db
    }

    // I spent about 45 minutes trying to use SQLite's WITH RECURSIVE feature and didn't get
    // quite what I wanted, and that's enough for this spike, I think
    public func path(rowid: Int, root: String) throws -> (String, String) {

        var components: [String] = []
        var current_item = SQLite.Blob(bytes: [])

        // convert the rowid into a item_blob_id
        if let item = try db.pluck(items.select(item_blob_id).where(self.rowid == rowid)) {
            current_item = item[item_blob_id]
        }

        // loop until we run out parents that exist
        while let row = try db.pluck(items.select(filename, item_parent_id).where(item_blob_id == current_item)) {
            current_item = row[item_parent_id]
            components.append(row[filename])
        }

        components.append(root)

        let filename = components.removeFirst()
        let directory = components.reversed().joined(separator: "/")
        return (directory, filename)
    }

    public func ctime_mtime(rowid: Int) throws -> (Date, Date) {
        let ctime_column = Expression<Int>("item_birthtime")
        let mtime_column = Expression<Int>("version_mtime")

        guard let row = try db.pluck(items.where(self.rowid == rowid)) else { throw Errs.noSuchItem }

        let ctime = row[ctime_column]
        let mtime = row[mtime_column]

        return (Date(timeIntervalSince1970: TimeInterval(ctime)),
                Date(timeIntervalSince1970: TimeInterval(mtime)))
    }

    public func source(rowid: Int) throws -> String? {
        guard (try db.pluck(items.where(self.rowid == rowid))) != nil else { throw Errs.noSuchItem }

        return sources[rowid]
    }

    public func process() throws {
        let fm = FileManager.default

        fm.createFile(atPath: "/tmp/errors.txt", contents: nil)
        fm.createFile(atPath: "/tmp/moves.txt", contents: nil)
        fm.createFile(atPath: "/tmp/missing.txt", contents: nil)
        fm.createFile(atPath: "/tmp/rowids.txt", contents: nil)

        guard let errorFile = FileHandle(forWritingAtPath: "/tmp/errors.txt"),
              let movedFile = FileHandle(forWritingAtPath: "/tmp/moves.txt"),
              let missingFile = FileHandle(forWritingAtPath: "/tmp/missing.txt"),
              let rowidsFile = FileHandle(forWritingAtPath: "/tmp/rowids.txt")
        else {
            print("can't open log files")
            return
        }

        let rowids = items.select(rowid, filename)
            .where(item_type == 1)
//            .where(rowid <= 1_000)
//            .limit(100)

        for row in try db.prepare(rowids) {
            let rowid = row[rowid]
            let filename = row[filename]
            try rowidsFile.write(contentsOf: "\(rowid)".data(using: .utf8)!)

            guard let (directory, filename) = try? path(rowid: rowid, root: "/path/to/output") else {
                print("Can't compute destination for '\(filename)'")
                try errorFile.write(contentsOf: "\(rowid): \(filename)\n".data(using: .utf8)!)
                continue
            }

            let destination = directory.appending("/").appending(filename)

            guard let source = try? source(rowid: rowid) else {
                try missingFile.write(contentsOf: "\(rowid): \(destination)\n".data(using: .utf8)!)
                continue
            }

            var ctime, mtime: Date
            (ctime, mtime) = try ctime_mtime(rowid: rowid)

            try fm.createDirectory(atPath: directory, withIntermediateDirectories: true)
            if fm.fileExists(atPath: destination) {
                try fm.removeItem(atPath: destination)
            }
            try fm.linkItem(atPath: source, toPath: destination)
            try fm.setAttributes([.creationDate: ctime, .modificationDate: mtime], ofItemAtPath: destination)

            try movedFile.write(contentsOf: "\(destination)\n".data(using: .utf8)!)
            try rowidsFile.write(contentsOf: " - ok\n".data(using: .utf8)!)
        }

        return
    }

}

var b = try Decloud()
try b.process()

