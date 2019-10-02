import Logging
import FluentKit
import FluentBenchmark
import FluentPostgresDriver
import XCTest

final class FluentPostgresDriverTests: XCTestCase {
    func testAll() throws {
        try self.benchmarker.testAll()
    }

    func testCreate() throws {
        try self.benchmarker.testCreate()
    }

    func testRead() throws {
        try self.benchmarker.testRead()
    }

    func testUpdate() throws {
        try self.benchmarker.testUpdate()
    }

    func testDelete() throws {
        try self.benchmarker.testDelete()
    }

    func testEagerLoadChildren() throws {
        try self.benchmarker.testEagerLoadChildren()
    }

    func testEagerLoadParent() throws {
        try self.benchmarker.testEagerLoadParent()
    }

    func testEagerLoadParentJoin() throws {
        try self.benchmarker.testEagerLoadParentJoin()
    }

    func testEagerLoadParentJSON() throws {
        try self.benchmarker.testEagerLoadParentJSON()
    }

    func testEagerLoadChildrenJSON() throws {
        try self.benchmarker.testEagerLoadChildrenJSON()
    }

    func testMigrator() throws {
        try self.benchmarker.testMigrator()
    }

    func testMigratorError() throws {
        try self.benchmarker.testMigratorError()
    }

    func testJoin() throws {
        try self.benchmarker.testJoin()
    }

    func testBatchCreate() throws {
        try self.benchmarker.testBatchCreate()
    }

    func testBatchUpdate() throws {
        try self.benchmarker.testBatchUpdate()
    }

    func testNestedModel() throws {
        try self.benchmarker.testNestedModel()
    }

    func testAggregates() throws {
        try self.benchmarker.testAggregates()
    }

    func testIdentifierGeneration() throws {
        try self.benchmarker.testIdentifierGeneration()
    }

    func testNullifyField() throws {
        try self.benchmarker.testNullifyField()
    }

    func testChunkedFetch() throws {
        try self.benchmarker.testChunkedFetch()
    }

    func testUniqueFields() throws {
        try self.benchmarker.testUniqueFields()
    }

    func testAsyncCreate() throws {
        try self.benchmarker.testAsyncCreate()
    }

    func testSoftDelete() throws {
        try self.benchmarker.testSoftDelete()
    }

    func testTimestampable() throws {
        try self.benchmarker.testTimestampable()
    }

    func testLifecycleHooks() throws {
        try self.benchmarker.testLifecycleHooks()
    }

    func testSort() throws {
        try self.benchmarker.testSort()
    }

    func testUUIDModel() throws {
        try self.benchmarker.testUUIDModel()
    }

    func testNewModelDecode() throws {
        try self.benchmarker.testNewModelDecode()
    }

    func testSiblingsAttach() throws {
        try self.benchmarker.testSiblingsAttach()
    }

    func testSiblingsEagerLoad() throws {
        try self.benchmarker.testSiblingsEagerLoad()
    }

    func testBlob() throws {
        final class Foo: Model {
            static let schema = "foos"

            @ID(key: "id")
            var id: Int?

            @Field(key: "data")
            var data: [UInt8]

            init() { }
        }

        struct CreateFoo: Migration {
            func prepare(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("foos")
                    .field("id", .int, .identifier(auto: true))
                    .field("data", .data, .required)
                    .create()
            }

            func revert(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("foos").delete()
            }
        }

        try CreateFoo().prepare(on: self.connectionPool).wait()
        try CreateFoo().revert(on: self.connectionPool).wait()
    }

    func testSaveModelWithBool() throws {
        final class Organization: Model {
            static let schema = "orgs"

            @ID(key: "id")
            var id: Int?

            @Field(key: "disabled")
            var disabled: Bool

            init() { }
        }

        struct CreateOrganization: Migration {
            func prepare(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("orgs")
                    .field("id", .int, .identifier(auto: true))
                    .field("disabled", .bool, .required)
                    .create()
            }

            func revert(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("orgs").delete()
            }
        }

        try CreateOrganization().prepare(on: self.connectionPool).wait()
        defer {
            try! CreateOrganization().revert(on: self.connectionPool).wait()
        }

        let new = Organization()
        new.disabled = false
        try new.save(on: self.connectionPool).wait()
    }

    func testLargeSave() throws {
        try? Galaxy.migration.revert(on: self.connectionPool).wait()
        try Galaxy.migration.prepare(on: self.connectionPool).wait()
        defer {
            try! Galaxy.migration.revert(on: self.connectionPool).wait()
        }
        for _ in 0..<10 {
            var galaxies: [Galaxy] = []
            for _ in 0..<1_000 {
                galaxies.append(Galaxy(name: "Test"))
            }
            try galaxies.create(on: self.connectionPool).wait()
        }

        try XCTAssertEqual(Galaxy.query(on: self.connectionPool).count().wait(), 10_000)
    }

    func testLargeSave2() throws {
        try? Galaxy.migration.revert(on: self.connectionPool).wait()
        try Galaxy.migration.prepare(on: self.connectionPool).wait()
        defer {
            try! Galaxy.migration.revert(on: self.connectionPool).wait()
        }
        for _ in 0..<1_000 {
            var galaxies: [[String: DatabaseQuery.Value]] = []
            for _ in 0..<1_000 {
                galaxies.append([
                    "id": .default,
                    "name": .bind("Test")
                ])
            }
            try Galaxy.query(on: self.connectionPool).set(galaxies).create().wait()
        }

        try XCTAssertEqual(Galaxy.query(on: self.connectionPool).count().wait(), 1_000_000)
    }

    func testLargeSave3() throws {
        try? Galaxy.migration.revert(on: self.connectionPool).wait()
        try Galaxy.migration.prepare(on: self.connectionPool).wait()
        defer {
            try! Galaxy.migration.revert(on: self.connectionPool).wait()
        }
        for _ in 0..<1_000 {
            var sql = """
            INSERT INTO "galaxies" ("id", "name") VALUES (DEFAULT, $1)
            """
            var binds: [PostgresData] = ["Test"]
            for i in 2...1_000 {
                sql.append(", (DEFAULT, $\(i))")
                binds.append("Test")
            }
            try self.connectionPool.query(sql, binds) { row in
                // ignore
            }.wait()
        }

        try XCTAssertEqual(Galaxy.query(on: self.connectionPool).count().wait(), 1_000_000)
    }

    var benchmarker: FluentBenchmarker!
    var connectionPool: ConnectionPool<PostgresConnectionSource>!
    var eventLoopGroup: EventLoopGroup!
    
    override func setUp() {
        XCTAssert(isLoggingConfigured)
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let eventLoop = eventLoopGroup.next()
        let hostname: String
        #if os(Linux)
        hostname = "psql"
        #else
        hostname = "localhost"
        #endif
        let configuration = PostgresConfiguration(
            hostname: hostname,
            port: 5432,
            username: "vapor_username",
            password: "vapor_password",
            database: "vapor_database",
            tlsConfiguration: nil
        )
        let db = PostgresConnectionSource(configuration: configuration, on: eventLoop)
        let pool = ConnectionPool(config: .init(maxConnections: 1), source: db)
        self.benchmarker = FluentBenchmarker(database: pool)
        self.connectionPool = pool
        self.eventLoopGroup = eventLoopGroup
    }

    override func tearDown() {
        try! self.connectionPool.close().wait()
        try! self.eventLoopGroup.syncShutdownGracefully()
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = .error
        return handler
    }
    return true
}()
