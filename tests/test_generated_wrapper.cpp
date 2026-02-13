/**
 * test_generated_wrapper.cpp
 *
 * Tests for struct-based entity wrappers with Glaze BEVE/JSON serialization.
 *
 *   1. TestUser       — basic entity (construction, field access, round-trips)
 *   2. TestArticle    — boolean, timestamp, nullable std::optional<T>
 *   3. TestPurchase   — cross-entity validation
 *   4. TestOrder      — comprehensive coverage: enum, nested struct, raw JSON,
 *                        vectors, nullable
 *   5. ListWrapper     — generic list wrapper (construction, serialization,
 *                        firstItem/lastItem, fromModels)
 *   6. Glaze vector   — validates Glaze round-trip for vector<Entity>
 *
 * SECTION naming convention:
 *   [Struct]       — direct struct construction and field access
 *   [Binary]       — BEVE binary round-trip (toBinary / fromBinary)
 *   [JSON]         — JSON round-trip (toJson / fromJson)
 *   [Model->Struct] — fromModel conversion
 *   [Struct->Model] — toModel conversion
 *   [Model<->Struct]— fromModel then toModel round-trip
 *   [List]         — ListWrapper construction / accessors
 *   [List->JSON]   — ListWrapper serialized to JSON
 */

#include <catch2/catch_test_macros.hpp>
#include <glaze/glaze.hpp>

#include "fixtures/generated/TestItemWrapper.h"
#include "fixtures/generated/TestUserWrapper.h"
#include "fixtures/generated/TestArticleWrapper.h"
#include "fixtures/generated/TestPurchaseWrapper.h"
#include "fixtures/generated/TestOrderWrapper.h"
#include <jcailloux/relais/wrapper/ListWrapper.h>

// Shadow raw struct names with EntityWrapper types for testing
using TestItem = entity::generated::TestItemWrapper;
using TestUser = entity::generated::TestUserWrapper;
using TestArticle = entity::generated::TestArticleWrapper;
using TestPurchase = entity::generated::TestPurchaseWrapper;
using TestOrder = entity::generated::TestOrderWrapper;
using relais_test::TestAddress;
using relais_test::TestGeoLocation;
using relais_test::TestCoordinateMetadata;
using relais_test::Priority;
using relais_test::Status;
using ListWrapperArticle = jcailloux::drogon::wrapper::ListWrapper<TestArticle>;
using ListWrapperItem = jcailloux::drogon::wrapper::ListWrapper<TestItem>;

// #############################################################################
//
//  1. TestUser — basic entity
//
// #############################################################################

TEST_CASE("TestUser - direct construction and field access", "[wrapper][struct][user]") {

    TestUser user;
    user.id = 42;
    user.username = "alice";
    user.email = "alice@example.com";
    user.balance = 1000;
    user.created_at = "2025-01-01T00:00:00Z";

    SECTION("[Struct] reads all fields") {
        REQUIRE(user.id == 42);
        REQUIRE(user.username == "alice");
        REQUIRE(user.email == "alice@example.com");
        REQUIRE(user.balance == 1000);
        REQUIRE(user.created_at == "2025-01-01T00:00:00Z");
    }

    SECTION("[Struct] getPrimaryKey returns id") {
        REQUIRE(user.getPrimaryKey() == 42);
    }
}

TEST_CASE("TestUser - binary (BEVE) round-trip", "[wrapper][binary][user]") {

    SECTION("[Binary] empty data returns nullopt") {
        REQUIRE_FALSE(TestUser::fromBinary({}).has_value());
    }

    SECTION("[Binary] invalid data returns nullopt") {
        std::vector<uint8_t> garbage = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02};
        REQUIRE_FALSE(TestUser::fromBinary(garbage).has_value());
    }

    TestUser user;
    user.id = 42;
    user.username = "alice";
    user.email = "alice@example.com";
    user.balance = 1000;
    user.created_at = "2025-01-01T00:00:00Z";

    SECTION("[Binary] round-trip preserves all fields") {
        auto restored = TestUser::fromBinary(*user.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 42);
        REQUIRE(restored->username == "alice");
        REQUIRE(restored->email == "alice@example.com");
        REQUIRE(restored->balance == 1000);
        REQUIRE(restored->created_at == "2025-01-01T00:00:00Z");
    }
}

TEST_CASE("TestUser - JSON round-trip", "[wrapper][json][user]") {

    TestUser user;
    user.id = 42;
    user.username = "alice";
    user.email = "alice@example.com";
    user.balance = 1000;
    user.created_at = "2025-01-01T00:00:00Z";

    SECTION("[JSON] toJson produces valid output") {
        auto json = user.toJson();
        REQUIRE(json);
        REQUIRE(json->find("\"id\":42") != std::string::npos);
        REQUIRE(json->find("\"username\":\"alice\"") != std::string::npos);
        REQUIRE(json->find("\"email\":\"alice@example.com\"") != std::string::npos);
        REQUIRE(json->find("\"balance\":1000") != std::string::npos);
    }

    SECTION("[JSON] result is cached (same pointer)") {
        auto p1 = user.toJson();
        auto p2 = user.toJson();
        REQUIRE(p1.get() == p2.get());
    }

    SECTION("[JSON] round-trip via fromJson") {
        auto json = user.toJson();
        auto restored = TestUser::fromJson(*json);
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 42);
        REQUIRE(restored->username == "alice");
        REQUIRE(restored->email == "alice@example.com");
        REQUIRE(restored->balance == 1000);
    }
}

TEST_CASE("TestUser - fromModel / toModel", "[wrapper][model][user]") {

    relais_test::TestUserModel model;
    model.setId(99);
    model.setUsername("bob");
    model.setEmail("bob@example.com");
    model.setBalance(500);
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-15 10:30:00"));
    auto user = TestUser::fromModel(model);

    SECTION("[Model->Struct] reads all fields") {
        REQUIRE(user.has_value());
        REQUIRE(user->id == 99);
        REQUIRE(user->username == "bob");
        REQUIRE(user->email == "bob@example.com");
        REQUIRE(user->balance == 500);
        REQUIRE_FALSE(user->created_at.empty());
    }

    SECTION("[Model->Struct] preserves zero numeric") {
        model.setBalance(0);
        auto u = TestUser::fromModel(model);
        REQUIRE(u.has_value());
        REQUIRE(u->balance == 0);
    }

    SECTION("[Struct->Model] reads non-DbManaged fields") {
        auto m = TestUser::toModel(*user);
        REQUIRE(m.getValueOfUsername() == "bob");
        REQUIRE(m.getValueOfEmail() == "bob@example.com");
        REQUIRE(m.getValueOfBalance() == 500);
    }

    SECTION("[Struct->Model] skips DbManaged id") {
        auto m = TestUser::toModel(*user);
        REQUIRE_FALSE(m.getId());
    }

    SECTION("[Struct->Model] converts timestamp") {
        auto m = TestUser::toModel(*user);
        REQUIRE(m.getCreatedAt());
    }

    SECTION("[Binary] round-trip after fromModel") {
        auto restored = TestUser::fromBinary(*user->toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 99);
        REQUIRE(restored->username == "bob");
    }
}

// #############################################################################
//
//  2. TestArticle — boolean, timestamp, nullable std::optional<T>
//
// #############################################################################

TEST_CASE("TestArticle - boolean and timestamp fields", "[wrapper][struct][article]") {

    relais_test::TestArticleModel model;
    model.setId(42);
    model.setCategory("tech");
    model.setAuthorId(7);
    model.setTitle("Hello World");
    model.setViewCount(100);
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-05-30 09:00:00"));

    SECTION("[Model->Struct] boolean true") {
        model.setIsPublished(true);
        auto a = TestArticle::fromModel(model);
        REQUIRE(a->is_published == true);
    }

    SECTION("[Model->Struct] boolean false") {
        model.setIsPublished(false);
        auto a = TestArticle::fromModel(model);
        REQUIRE(a->is_published == false);
    }

    SECTION("[Model->Struct] all fields including timestamps") {
        model.setIsPublished(true);
        model.setPublishedAt(trantor::Date::fromDbStringLocal("2025-06-01 12:00:00"));
        auto a = TestArticle::fromModel(model);
        REQUIRE(a.has_value());
        REQUIRE(a->id == 42);
        REQUIRE(a->category == "tech");
        REQUIRE(a->author_id == 7);
        REQUIRE(a->title == "Hello World");
        REQUIRE(a->view_count.has_value());
        REQUIRE(*a->view_count == 100);
        REQUIRE(a->is_published == true);
        REQUIRE(a->published_at.has_value());
        REQUIRE_FALSE(a->published_at->empty());
        REQUIRE_FALSE(a->created_at.empty());
    }
}

TEST_CASE("TestArticle - nullable fields", "[wrapper][struct][article][nullable]") {

    TestArticle article;
    article.id = 1;
    article.category = "tech";
    article.author_id = 7;
    article.title = "Test";
    article.is_published = false;
    article.created_at = "2025-01-01T00:00:00Z";
    // view_count intentionally not set (std::nullopt by default)

    SECTION("[Struct] absent value is nullopt") {
        REQUIRE_FALSE(article.view_count.has_value());
    }

    SECTION("[JSON] absent optional is handled") {
        auto json = article.toJson();
        REQUIRE(json);
        // Glaze serializes std::optional as null or omits it depending on config
    }

    SECTION("[Struct->Model] absent value leaves model null") {
        REQUIRE_FALSE(TestArticle::toModel(article).getViewCount());
    }

    SECTION("[Struct] explicit 0 returns optional(0)") {
        article.view_count = 0;
        REQUIRE(article.view_count.has_value());
        REQUIRE(*article.view_count == 0);
    }

    SECTION("[Struct->Model] explicit 0 sets value") {
        article.view_count = 0;
        auto m = TestArticle::toModel(article);
        REQUIRE(m.getViewCount());
        REQUIRE(*m.getViewCount() == 0);
    }

    SECTION("[Struct] non-zero value returns optional(42)") {
        article.view_count = 42;
        REQUIRE(*article.view_count == 42);
    }

    SECTION("[Binary] round-trip preserves absent optional") {
        auto restored = TestArticle::fromBinary(*article.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE_FALSE(restored->view_count.has_value());
    }

    SECTION("[Binary] round-trip preserves present optional") {
        article.view_count = 42;
        auto restored = TestArticle::fromBinary(*article.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->view_count.has_value());
        REQUIRE(*restored->view_count == 42);
    }
}

TEST_CASE("TestArticle - nullable from model", "[wrapper][model][article][nullable]") {

    relais_test::TestArticleModel model;
    model.setId(20);
    model.setCategory("tech");
    model.setAuthorId(1);
    model.setTitle("Test");
    model.setIsPublished(false);
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-01-01 00:00:00"));

    SECTION("[Model->Struct] null produces absent") {
        auto a = TestArticle::fromModel(model);
        REQUIRE_FALSE(a->view_count.has_value());
    }

    SECTION("[Model->Struct] 0 produces optional(0)") {
        model.setViewCount(0);
        auto a = TestArticle::fromModel(model);
        REQUIRE(a->view_count.has_value());
        REQUIRE(*a->view_count == 0);
    }

    SECTION("[Model->Struct] 100 produces optional(100)") {
        model.setViewCount(100);
        auto a = TestArticle::fromModel(model);
        REQUIRE(*a->view_count == 100);
    }
}

// #############################################################################
//
//  3. TestPurchase — cross-entity validation
//
// #############################################################################

TEST_CASE("TestPurchase - fromModel / toModel / toJson", "[wrapper][struct][purchase]") {

    relais_test::TestPurchaseModel model;
    model.setId(1);
    model.setUserId(42);
    model.setProductName("Widget");
    model.setAmount(999);
    model.setStatus("completed");
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-01-01 00:00:00"));
    auto purchase = TestPurchase::fromModel(model);

    SECTION("[Model->Struct] reads all fields") {
        REQUIRE(purchase.has_value());
        REQUIRE(purchase->id == 1);
        REQUIRE(purchase->user_id == 42);
        REQUIRE(purchase->product_name == "Widget");
        REQUIRE(purchase->amount == 999);
        REQUIRE(purchase->status == "completed");
    }

    SECTION("[Struct->Model] round-trip") {
        auto m = TestPurchase::toModel(*purchase);
        REQUIRE(m.getValueOfUserId() == 42);
        REQUIRE(m.getValueOfProductName() == "Widget");
        REQUIRE(m.getValueOfAmount() == 999);
        REQUIRE(m.getValueOfStatus() == "completed");
    }

    SECTION("[JSON] contains all fields") {
        auto json = purchase->toJson();
        REQUIRE(json->find("\"user_id\":42") != std::string::npos);
        REQUIRE(json->find("\"product_name\":\"Widget\"") != std::string::npos);
        REQUIRE(json->find("\"amount\":999") != std::string::npos);
    }

    SECTION("[Binary] round-trip preserves data") {
        auto restored = TestPurchase::fromBinary(*purchase->toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 1);
        REQUIRE(restored->user_id == 42);
        REQUIRE(restored->product_name == "Widget");
        REQUIRE(restored->amount == 999);
        REQUIRE(restored->status == "completed");
    }
}

// #############################################################################
//
//  4. TestOrder — comprehensive coverage of all field types
//
//  Covers:
//    Numeric      — id (PK+DbManaged), user_id, amount, is_express (bool)
//    String       — label, created_at (Timestamp)
//    RawJson      — metadata (glz::raw_json_t)
//    Enum         — priority (Priority)
//    Nested struct— address (TestAddress with 4-level nesting)
//    Object vector— history (vector<TestAddress>)
//    Scalar vector— quantities (vector<int32_t>)
//    String vector— tags (vector<string>)
//    Nullable     — discount (optional<int32_t>)
//
// #############################################################################

namespace {

/// Build a TestOrder struct with ALL fields populated.
TestOrder buildFullTestOrder() {
    TestOrder order;
    order.id = 100;
    order.user_id = 42;
    order.amount = 999;
    order.discount = 50;
    order.is_express = true;
    order.priority = Priority::High;
    order.status = Status::Shipped;
    order.label = "rush-order";
    order.metadata.str = R"({"x":1})";
    order.created_at = "2025-07-01T12:00:00Z";

    // Nested address with 4-level nesting: Order -> Address -> GeoLocation -> CoordinateMetadata
    order.address.street = "123 Main St";
    order.address.city = "Paris";
    order.address.zip_code = "75001";
    order.address.geo.latitude = 48.8566;
    order.address.geo.longitude = 2.3522;
    order.address.geo.metadata.accuracy = 1.5f;
    order.address.geo.metadata.source = "gps";

    // History (vector of addresses)
    TestAddress h1;
    h1.street = "10 Rue A";
    h1.city = "Lyon";
    h1.zip_code = "69001";
    TestAddress h2;
    h2.street = "20 Rue B";
    h2.city = "Marseille";
    h2.zip_code = "13001";
    order.history = {h1, h2};

    // Scalar and string vectors
    order.quantities = {10, 20, 30};
    order.tags = {"urgent", "fragile"};

    return order;
}

/// Build a TestOrder with only scalar/string fields (no composites).
TestOrder buildMinimalTestOrder() {
    TestOrder order;
    order.id = 1;
    order.user_id = 1;
    order.amount = 100;
    order.is_express = false;
    order.priority = Priority::Low;
    order.status = Status::Pending;
    order.label = "test";
    order.created_at = "2025-01-01T00:00:00Z";
    return order;
}

/// Build a Drogon model with all scalar/string/enum fields set.
drogon_model::relais_test::Mock_RelaisTestOrders buildTestOrderModel(
        int64_t id = 1, const std::string& priority = "low") {
    drogon_model::relais_test::Mock_RelaisTestOrders model;
    model.setId(id);
    model.setUserId(1);
    model.setAmount(100);
    model.setIsExpress(false);
    model.setPriority(priority);
    model.setStatus("pending");
    model.setLabel("test");
    model.setMetadata("");
    model.setAddress("");
    model.setHistory("[]");
    model.setQuantities("[]");
    model.setTags("[]");
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-01-01 00:00:00"));
    return model;
}

}  // anonymous namespace

TEST_CASE("TestOrder - direct construction reads all fields", "[wrapper][struct][order]") {

    auto order = buildFullTestOrder();

    REQUIRE(order.id == 100);
    REQUIRE(order.user_id == 42);
    REQUIRE(order.amount == 999);
    REQUIRE(order.discount.has_value());
    REQUIRE(*order.discount == 50);
    REQUIRE(order.is_express == true);
    REQUIRE(order.priority == Priority::High);
    REQUIRE(order.status == Status::Shipped);
    REQUIRE(order.label == "rush-order");
    REQUIRE(order.metadata.str == R"({"x":1})");
    REQUIRE(order.created_at == "2025-07-01T12:00:00Z");

    // Nested struct fields
    REQUIRE(order.address.street == "123 Main St");
    REQUIRE(order.address.city == "Paris");
    REQUIRE(order.address.zip_code == "75001");
    REQUIRE(order.address.geo.latitude == 48.8566);
    REQUIRE(order.address.geo.longitude == 2.3522);
    REQUIRE(order.address.geo.metadata.accuracy == 1.5f);
    REQUIRE(order.address.geo.metadata.source == "gps");

    // Vectors
    REQUIRE(order.history.size() == 2);
    REQUIRE(order.quantities.size() == 3);
    REQUIRE(order.tags.size() == 2);
}

TEST_CASE("TestOrder - binary (BEVE) round-trip", "[wrapper][binary][order]") {

    auto order = buildFullTestOrder();
    auto restored = TestOrder::fromBinary(*order.toBinary());
    REQUIRE(restored.has_value());

    SECTION("[Binary] preserves scalar fields") {
        REQUIRE(restored->id == 100);
        REQUIRE(restored->user_id == 42);
        REQUIRE(restored->amount == 999);
        REQUIRE(restored->is_express == true);
        REQUIRE(restored->label == "rush-order");
        REQUIRE(restored->created_at == "2025-07-01T12:00:00Z");
    }

    SECTION("[Binary] preserves nullable field") {
        REQUIRE(restored->discount.has_value());
        REQUIRE(*restored->discount == 50);
    }

    SECTION("[Binary] preserves enum fields") {
        REQUIRE(restored->priority == Priority::High);
        REQUIRE(restored->status == Status::Shipped);
    }

    SECTION("[Binary] preserves nested struct (4 levels)") {
        REQUIRE(restored->address.street == "123 Main St");
        REQUIRE(restored->address.city == "Paris");
        REQUIRE(restored->address.zip_code == "75001");
        REQUIRE(restored->address.geo.latitude == 48.8566);
        REQUIRE(restored->address.geo.longitude == 2.3522);
        REQUIRE(restored->address.geo.metadata.accuracy == 1.5f);
        REQUIRE(restored->address.geo.metadata.source == "gps");
    }

    SECTION("[Binary] preserves vector fields") {
        REQUIRE(restored->history.size() == 2);
        REQUIRE(restored->history[0].street == "10 Rue A");
        REQUIRE(restored->history[1].city == "Marseille");
        REQUIRE(restored->quantities.size() == 3);
        REQUIRE(restored->quantities[0] == 10);
        REQUIRE(restored->quantities[1] == 20);
        REQUIRE(restored->quantities[2] == 30);
        REQUIRE(restored->tags.size() == 2);
        REQUIRE(restored->tags[0] == "urgent");
        REQUIRE(restored->tags[1] == "fragile");
    }

    SECTION("[Binary] preserves raw JSON metadata") {
        REQUIRE(restored->metadata.str == R"({"x":1})");
    }
}

TEST_CASE("TestOrder - EnumField (priority)", "[wrapper][struct][order][enum]") {

    auto order = buildFullTestOrder();

    SECTION("[Struct] accessor returns enum value") {
        REQUIRE(order.priority == Priority::High);
    }

    SECTION("[JSON] outputs quoted lowercase string") {
        REQUIRE(order.toJson()->find("\"priority\":\"high\"") != std::string::npos);
    }

    SECTION("[Struct->Model] converts enum to lowercase string") {
        auto m = TestOrder::toModel(order);
        REQUIRE(m.getValueOfPriority() == "high");
    }

    SECTION("[Model->Struct] converts string to enum") {
        auto model = buildTestOrderModel(1, "medium");
        auto o = TestOrder::fromModel(model);
        REQUIRE(o.has_value());
        REQUIRE(o->priority == Priority::Medium);
    }
}

TEST_CASE("TestOrder - EnumField (status — developer-defined glz::meta)", "[wrapper][struct][order][enum]") {

    auto order = buildFullTestOrder();

    SECTION("[Struct] accessor returns enum value") {
        REQUIRE(order.status == Status::Shipped);
    }

    SECTION("[JSON] outputs quoted lowercase string") {
        REQUIRE(order.toJson()->find("\"status\":\"shipped\"") != std::string::npos);
    }

    SECTION("[Binary] round-trips through BEVE") {
        auto restored = TestOrder::fromBinary(*order.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->status == Status::Shipped);
    }

    SECTION("[Struct->Model] converts enum to lowercase string") {
        auto m = TestOrder::toModel(order);
        REQUIRE(m.getValueOfStatus() == "shipped");
    }

    SECTION("[Model->Struct] converts string to enum") {
        auto model = buildTestOrderModel();
        model.setStatus("delivered");
        auto o = TestOrder::fromModel(model);
        REQUIRE(o.has_value());
        REQUIRE(o->status == Status::Delivered);
    }
}

TEST_CASE("TestOrder - RawJson (metadata)", "[wrapper][struct][order][rawjson]") {

    SECTION("[JSON] injects raw JSON correctly") {
        auto order = buildFullTestOrder();
        REQUIRE(order.toJson()->find("\"metadata\":{\"x\":1}") != std::string::npos);
    }

    SECTION("[Model<->Struct] preserve raw string") {
        auto model = buildTestOrderModel();
        model.setMetadata(R"({"key":"value"})");
        auto o = TestOrder::fromModel(model);
        REQUIRE(o.has_value());
        auto m = TestOrder::toModel(*o);
        REQUIRE(m.getValueOfMetadata() == R"({"key":"value"})");
    }
}

TEST_CASE("TestOrder - nested struct (address)", "[wrapper][struct][order][object]") {

    SECTION("[JSON] outputs nested object with 4-level nesting") {
        auto order = buildFullTestOrder();
        auto json = *order.toJson();
        REQUIRE(json.find("\"street\":\"123 Main St\"") != std::string::npos);
        REQUIRE(json.find("\"latitude\":") != std::string::npos);
        REQUIRE(json.find("\"source\":\"gps\"") != std::string::npos);
    }

    SECTION("[Struct] manual access traverses 4 levels") {
        auto order = buildFullTestOrder();
        REQUIRE(order.address.street == "123 Main St");
        REQUIRE(order.address.city == "Paris");
        REQUIRE(order.address.zip_code == "75001");
        // Level 3: GeoLocation
        REQUIRE(order.address.geo.latitude == 48.8566);
        REQUIRE(order.address.geo.longitude == 2.3522);
        // Level 4: CoordinateMetadata
        REQUIRE(order.address.geo.metadata.accuracy == 1.5f);
        REQUIRE(order.address.geo.metadata.source == "gps");
    }

    SECTION("[Model->Struct] address from JSON string (4 levels)") {
        auto model = buildTestOrderModel();
        model.setAddress(
            R"({"street":"123 Main St","city":"Paris","zip_code":"75001","geo":{"latitude":48.8566,"longitude":2.3522,"metadata":{"accuracy":1.5,"source":"gps"}}})");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        REQUIRE(order->address.street == "123 Main St");
        REQUIRE(order->address.city == "Paris");
        REQUIRE(order->address.zip_code == "75001");
        REQUIRE(order->address.geo.latitude == 48.8566);
        REQUIRE(order->address.geo.longitude == 2.3522);
        REQUIRE(order->address.geo.metadata.accuracy == 1.5f);
        REQUIRE(order->address.geo.metadata.source == "gps");
    }
}

TEST_CASE("TestOrder - ObjectVectorField (history)", "[wrapper][struct][order][objectvec]") {

    SECTION("[JSON] outputs array of objects") {
        auto order = buildFullTestOrder();
        auto json = order.toJson();
        REQUIRE(json->find("\"street\":\"10 Rue A\"") != std::string::npos);
        REQUIRE(json->find("\"street\":\"20 Rue B\"") != std::string::npos);
    }

    SECTION("[Model->Struct] history from JSON array") {
        auto model = buildTestOrderModel();
        model.setHistory(
            R"([{"street":"10 Rue A","city":"Lyon","zip_code":"69001"},{"street":"20 Rue B","city":"Marseille","zip_code":"13001"}])");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        REQUIRE(order->history.size() == 2);
        REQUIRE(order->history[0].street == "10 Rue A");
        REQUIRE(order->history[1].city == "Marseille");
    }

    SECTION("[Model->Struct] history with nested geo (deep nesting in vector)") {
        auto model = buildTestOrderModel();
        model.setHistory(
            R"([{"street":"42 Av C","city":"Nice","zip_code":"06000","geo":{"latitude":43.7,"longitude":7.27,"metadata":{"accuracy":2.0,"source":"wifi"}}}])");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        REQUIRE(order->history.size() == 1);
        REQUIRE(order->history[0].street == "42 Av C");
        REQUIRE(order->history[0].geo.latitude == 43.7);
        REQUIRE(order->history[0].geo.metadata.source == "wifi");
    }
}

TEST_CASE("TestOrder - ScalarVectorField (quantities)", "[wrapper][struct][order][scalarvec]") {

    SECTION("[JSON] outputs array of numbers") {
        auto order = buildFullTestOrder();
        REQUIRE(order.toJson()->find("\"quantities\":[10,20,30]") != std::string::npos);
    }

    SECTION("[Model->Struct] quantities from JSON array") {
        auto model = buildTestOrderModel();
        model.setQuantities("[10,20,30]");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        REQUIRE(order->quantities.size() == 3);
        REQUIRE(order->quantities[0] == 10);
        REQUIRE(order->quantities[1] == 20);
        REQUIRE(order->quantities[2] == 30);
    }
}

TEST_CASE("TestOrder - StringVectorField (tags)", "[wrapper][struct][order][stringvec]") {

    SECTION("[JSON] outputs array of strings") {
        auto order = buildFullTestOrder();
        REQUIRE(order.toJson()->find("\"tags\":[\"urgent\",\"fragile\"]") != std::string::npos);
    }

    SECTION("[Model->Struct] tags from JSON array") {
        auto model = buildTestOrderModel();
        model.setTags(R"(["urgent","fragile"])");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        REQUIRE(order->tags.size() == 2);
        REQUIRE(order->tags[0] == "urgent");
        REQUIRE(order->tags[1] == "fragile");
    }
}

TEST_CASE("TestOrder - nullable discount", "[wrapper][struct][order][nullable]") {

    SECTION("[Struct] absent returns nullopt") {
        auto order = buildMinimalTestOrder();
        REQUIRE_FALSE(order.discount.has_value());
    }

    SECTION("[Struct] explicit 0 returns optional(0)") {
        auto order = buildMinimalTestOrder();
        order.discount = 0;
        REQUIRE(order.discount.has_value());
        REQUIRE(*order.discount == 0);
    }

    SECTION("[Struct] non-zero value returns optional(50)") {
        auto order = buildFullTestOrder();
        REQUIRE(order.discount.has_value());
        REQUIRE(*order.discount == 50);
    }

    SECTION("[Binary] round-trip preserves absent") {
        auto order = buildMinimalTestOrder();
        auto restored = TestOrder::fromBinary(*order.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE_FALSE(restored->discount.has_value());
    }

    SECTION("[Binary] round-trip preserves present value") {
        auto order = buildFullTestOrder();
        auto restored = TestOrder::fromBinary(*order.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->discount.has_value());
        REQUIRE(*restored->discount == 50);
    }

    SECTION("[Model->Struct] null produces absent") {
        auto model = buildTestOrderModel();
        auto o = TestOrder::fromModel(model);
        REQUIRE_FALSE(o->discount.has_value());
    }

    SECTION("[Model->Struct] 0 produces optional(0)") {
        auto model = buildTestOrderModel();
        model.setDiscount(0);
        auto o = TestOrder::fromModel(model);
        REQUIRE(o->discount.has_value());
        REQUIRE(*o->discount == 0);
    }
}

TEST_CASE("TestOrder - fromModel / toModel round-trip", "[wrapper][model][order]") {

    drogon_model::relais_test::Mock_RelaisTestOrders model;
    model.setId(55);
    model.setUserId(42);
    model.setAmount(999);
    model.setDiscount(25);
    model.setIsExpress(true);
    model.setPriority("critical");
    model.setStatus("delivered");
    model.setLabel("rush");
    model.setMetadata(R"({"foo":"bar"})");
    model.setAddress(
        R"({"street":"A","city":"B","zip_code":"C","geo":{"latitude":1.0,"longitude":2.0,"metadata":{"accuracy":3.0,"source":"test"}}})");
    model.setHistory(R"([{"street":"D","city":"E","zip_code":"F"}])");
    model.setQuantities("[5,10]");
    model.setTags(R"(["a","b","c"])");
    model.setCreatedAt(trantor::Date::fromDbStringLocal("2025-07-15 09:30:00"));
    auto order = TestOrder::fromModel(model);
    REQUIRE(order.has_value());

    SECTION("[Model->Struct] reads all scalar/string/enum fields") {
        REQUIRE(order->id == 55);
        REQUIRE(order->user_id == 42);
        REQUIRE(order->amount == 999);
        REQUIRE(order->discount.has_value());
        REQUIRE(*order->discount == 25);
        REQUIRE(order->is_express == true);
        REQUIRE(order->priority == Priority::Critical);
        REQUIRE(order->status == Status::Delivered);
        REQUIRE(order->label == "rush");
        REQUIRE(order->metadata.str == R"({"foo":"bar"})");
        REQUIRE_FALSE(order->created_at.empty());
    }

    SECTION("[Model->Struct] reads composite fields") {
        REQUIRE(order->address.street == "A");
        REQUIRE(order->address.city == "B");
        REQUIRE(order->address.zip_code == "C");
        REQUIRE(order->address.geo.latitude == 1.0);
        REQUIRE(order->address.geo.longitude == 2.0);
        REQUIRE(order->address.geo.metadata.accuracy == 3.0f);
        REQUIRE(order->address.geo.metadata.source == "test");
        REQUIRE(order->history.size() == 1);
        REQUIRE(order->history[0].street == "D");
        REQUIRE(order->quantities.size() == 2);
        REQUIRE(order->quantities[0] == 5);
        REQUIRE(order->quantities[1] == 10);
        REQUIRE(order->tags.size() == 3);
        REQUIRE(order->tags[0] == "a");
        REQUIRE(order->tags[1] == "b");
        REQUIRE(order->tags[2] == "c");
    }

    SECTION("[Struct->Model] skips DbManaged id") {
        auto m = TestOrder::toModel(*order);
        REQUIRE_FALSE(m.getId());
    }

    SECTION("[Struct->Model] round-trips scalar fields") {
        auto m = TestOrder::toModel(*order);
        REQUIRE(m.getValueOfUserId() == 42);
        REQUIRE(m.getValueOfAmount() == 999);
        REQUIRE(m.getValueOfIsExpress() == true);
    }

    SECTION("[Struct->Model] round-trips nullable discount") {
        auto m = TestOrder::toModel(*order);
        REQUIRE(m.getDiscount());
        REQUIRE(*m.getDiscount() == 25);
    }

    SECTION("[Struct->Model] round-trips enums as strings") {
        auto m = TestOrder::toModel(*order);
        REQUIRE(m.getValueOfPriority() == "critical");
        REQUIRE(m.getValueOfStatus() == "delivered");
    }

    SECTION("[Struct->Model] round-trips string fields") {
        auto m = TestOrder::toModel(*order);
        REQUIRE(m.getValueOfLabel() == "rush");
        REQUIRE(m.getValueOfMetadata() == R"({"foo":"bar"})");
    }

    SECTION("[Struct->Model] round-trips timestamp") {
        auto m = TestOrder::toModel(*order);
        REQUIRE(m.getCreatedAt());
    }
}

TEST_CASE("TestOrder - deep nesting round-trip (4 levels)", "[wrapper][struct][order][deep]") {

    SECTION("[Model->Struct->JSON] round-trip 4 levels via JSON string") {
        auto model = buildTestOrderModel();
        model.setAddress(
            R"({"street":"1 Rue X","city":"Lille","zip_code":"59000","geo":{"latitude":50.63,"longitude":3.06,"metadata":{"accuracy":0.5,"source":"satellite"}}})");
        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());
        auto json = *order->toJson();
        REQUIRE(json.find("\"street\":\"1 Rue X\"") != std::string::npos);
        REQUIRE(json.find("\"latitude\":") != std::string::npos);
        REQUIRE(json.find("\"source\":\"satellite\"") != std::string::npos);
    }

    SECTION("[Model->Struct] full composite round-trip") {
        auto model = buildTestOrderModel();
        model.setAddress(
            R"({"street":"A","city":"B","zip_code":"C","geo":{"latitude":1.0,"longitude":2.0,"metadata":{"accuracy":3.0,"source":"test"}}})");
        model.setHistory(R"([{"street":"D","city":"E","zip_code":"F"}])");
        model.setQuantities("[5,10]");
        model.setTags(R"(["a","b","c"])");

        auto order = TestOrder::fromModel(model);
        REQUIRE(order.has_value());

        // Verify all composites are present
        REQUIRE(order->address.street == "A");
        REQUIRE(order->address.geo.latitude == 1.0);
        REQUIRE(order->address.geo.metadata.source == "test");
        REQUIRE(order->history.size() == 1);
        REQUIRE(order->quantities.size() == 2);
        REQUIRE(order->tags.size() == 3);
    }
}

// #############################################################################
//
//  5. ListWrapper — generic list wrapper
//
// #############################################################################

TEST_CASE("ListWrapper<TestArticle> - construction and accessors", "[wrapper][list][article]") {

    SECTION("[List] fromBinary with empty data returns nullopt") {
        REQUIRE_FALSE(ListWrapperArticle::fromBinary({}).has_value());
    }

    SECTION("[List] fromBinary with invalid data returns nullopt") {
        std::vector<uint8_t> garbage = {0xFF, 0xFF};
        REQUIRE_FALSE(ListWrapperArticle::fromBinary(garbage).has_value());
    }

    SECTION("[List] fromModels with empty list") {
        auto list = ListWrapperArticle::fromModels({});
        REQUIRE(list.size() == 0);
        REQUIRE(list.totalCount() == 0);
        REQUIRE(list.empty());
        REQUIRE(list.nextCursor().empty());
    }

    relais_test::TestArticleModel m1;
    m1.setId(1);
    m1.setCategory("tech");
    m1.setAuthorId(7);
    m1.setTitle("First");
    m1.setIsPublished(true);
    m1.setViewCount(10);
    m1.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-01 00:00:00"));

    relais_test::TestArticleModel m2;
    m2.setId(2);
    m2.setCategory("science");
    m2.setAuthorId(3);
    m2.setTitle("Second");
    m2.setIsPublished(false);
    m2.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-02 00:00:00"));

    auto list = ListWrapperArticle::fromModels({m1, m2});

    SECTION("[List] fromModels size and totalCount") {
        REQUIRE(list.size() == 2);
        REQUIRE(list.totalCount() == 2);
        REQUIRE_FALSE(list.empty());
    }

    SECTION("[List] firstItem returns pointer to first item") {
        auto* first = list.firstItem();
        REQUIRE(first != nullptr);
        REQUIRE(first->category == "tech");
        REQUIRE(first->author_id == 7);
        REQUIRE(first->view_count.has_value());
        REQUIRE(*first->view_count == 10);
    }

    SECTION("[List] lastItem returns pointer to last item") {
        auto* last = list.lastItem();
        REQUIRE(last != nullptr);
        REQUIRE(last->category == "science");
        REQUIRE(last->author_id == 3);
    }

    SECTION("[List] nullable absent in list item") {
        auto* last = list.lastItem();
        REQUIRE_FALSE(last->view_count.has_value());
    }

    SECTION("[List] toBinary round-trip preserves list") {
        auto restored = ListWrapperArticle::fromBinary(*list.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->size() == 2);
    }

    SECTION("[List] items vector is directly accessible") {
        REQUIRE(list.items.size() == 2);
        REQUIRE(list.items[0].id == 1);
        REQUIRE(list.items[1].id == 2);
    }

    SECTION("[List] total_count is directly accessible") {
        REQUIRE(list.total_count == 2);
    }

    // --- fromItems ---

    auto e1 = std::make_shared<const TestArticle>(*TestArticle::fromModel(m1));
    auto e2 = std::make_shared<const TestArticle>(*TestArticle::fromModel(m2));
    std::vector<std::shared_ptr<const TestArticle>> items = {e1, e2};

    SECTION("[List] fromItems size") {
        auto from_items = ListWrapperArticle::fromItems(items);
        REQUIRE(from_items.size() == 2);
    }

    SECTION("[List] fromItems with cursor") {
        auto from_items = ListWrapperArticle::fromItems(items, "cursor_abc");
        REQUIRE(from_items.nextCursor() == "cursor_abc");
    }

    SECTION("[List] fromItems preserves nullable present") {
        std::vector<std::shared_ptr<const TestArticle>> one = {e1};
        auto from_items = ListWrapperArticle::fromItems(one);
        auto* first = from_items.firstItem();
        REQUIRE(first != nullptr);
        REQUIRE(first->view_count.has_value());
        REQUIRE(*first->view_count == 10);
    }

    SECTION("[List] fromItems preserves nullable absent") {
        std::vector<std::shared_ptr<const TestArticle>> one = {e2};
        auto from_items = ListWrapperArticle::fromItems(one);
        auto* first = from_items.firstItem();
        REQUIRE(first != nullptr);
        REQUIRE_FALSE(first->view_count.has_value());
    }
}

TEST_CASE("ListWrapper<TestArticle> - toJson", "[wrapper][list][article][json]") {

    SECTION("[List->JSON] empty list") {
        auto list = ListWrapperArticle::fromModels({});
        auto json = list.toJson();
        REQUIRE(json);
        REQUIRE(json->find("\"items\":[]") != std::string::npos);
    }

    relais_test::TestArticleModel m;
    m.setId(1);
    m.setCategory("tech");
    m.setAuthorId(7);
    m.setTitle("Test");
    m.setIsPublished(true);
    m.setViewCount(42);
    m.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-01 00:00:00"));

    auto list = ListWrapperArticle::fromModels({m});

    SECTION("[List->JSON] items are serialized") {
        auto json = list.toJson();
        REQUIRE(json->find("\"items\":[{") != std::string::npos);
        REQUIRE(json->find("\"view_count\":42") != std::string::npos);
        REQUIRE(json->find("\"category\":\"tech\"") != std::string::npos);
    }

    SECTION("[List->JSON] result is cached (same pointer)") {
        auto p1 = list.toJson();
        auto p2 = list.toJson();
        REQUIRE(p1.get() == p2.get());
    }
}

TEST_CASE("ListWrapper<TestArticle> - JSON round-trip", "[wrapper][list][article][json]") {

    relais_test::TestArticleModel m;
    m.setId(1);
    m.setCategory("tech");
    m.setAuthorId(7);
    m.setTitle("Test");
    m.setIsPublished(true);
    m.setViewCount(42);
    m.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-01 00:00:00"));

    auto list = ListWrapperArticle::fromModels({m});
    auto json = list.toJson();

    SECTION("[List] fromJson round-trip") {
        auto restored = ListWrapperArticle::fromJson(*json);
        REQUIRE(restored.has_value());
        REQUIRE(restored->size() == 1);
        auto* first = restored->firstItem();
        REQUIRE(first != nullptr);
        REQUIRE(first->category == "tech");
        REQUIRE(first->view_count.has_value());
        REQUIRE(*first->view_count == 42);
    }
}

// #############################################################################
//
//  6. Glaze vector round-trip — validates Glaze serialization paths
//
// #############################################################################

TEST_CASE("Glaze vector round-trip - TestUser", "[wrapper][glaze][user]") {

    relais_test::TestUserModel m1;
    m1.setId(1);
    m1.setUsername("alice");
    m1.setEmail("alice@test.com");
    m1.setBalance(100);
    m1.setCreatedAt(trantor::Date::fromDbStringLocal("2025-01-01 00:00:00"));

    relais_test::TestUserModel m2;
    m2.setId(2);
    m2.setUsername("bob");
    m2.setEmail("bob@test.com");
    m2.setBalance(0);
    m2.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-15 10:30:00"));

    std::vector<TestUser> original = {
        *TestUser::fromModel(m1),
        *TestUser::fromModel(m2)
    };

    std::string json;
    REQUIRE_FALSE(glz::write_json(original, json));

    SECTION("[Glaze] write_json produces valid JSON array") {
        REQUIRE(json.front() == '[');
        REQUIRE(json.back() == ']');
        REQUIRE(json.find("\"username\":\"alice\"") != std::string::npos);
        REQUIRE(json.find("\"username\":\"bob\"") != std::string::npos);
    }

    SECTION("[Glaze] read_json round-trip preserves all fields") {
        std::vector<TestUser> restored;
        REQUIRE_FALSE(glz::read_json(restored, json));
        REQUIRE(restored.size() == 2);
        REQUIRE(restored[0].id == 1);
        REQUIRE(restored[0].username == "alice");
        REQUIRE(restored[0].email == "alice@test.com");
        REQUIRE(restored[0].balance == 100);
        REQUIRE_FALSE(restored[0].created_at.empty());
        REQUIRE(restored[1].id == 2);
        REQUIRE(restored[1].username == "bob");
        REQUIRE(restored[1].balance == 0);
    }
}

TEST_CASE("Glaze vector round-trip - TestArticle (nullable)", "[wrapper][glaze][article]") {

    relais_test::TestArticleModel m1;
    m1.setId(10);
    m1.setCategory("tech");
    m1.setAuthorId(7);
    m1.setTitle("With views");
    m1.setViewCount(42);
    m1.setIsPublished(true);
    m1.setPublishedAt(trantor::Date::fromDbStringLocal("2025-06-01 12:00:00"));
    m1.setCreatedAt(trantor::Date::fromDbStringLocal("2025-05-30 09:00:00"));

    relais_test::TestArticleModel m2;
    m2.setId(20);
    m2.setCategory("science");
    m2.setAuthorId(3);
    m2.setTitle("No views");
    m2.setIsPublished(false);
    m2.setCreatedAt(trantor::Date::fromDbStringLocal("2025-06-02 00:00:00"));
    // view_count intentionally not set (nullable absent)

    std::vector<TestArticle> original = {
        *TestArticle::fromModel(m1),
        *TestArticle::fromModel(m2)
    };

    std::string json;
    REQUIRE_FALSE(glz::write_json(original, json));

    SECTION("[Glaze] round-trip preserves nullable present value") {
        std::vector<TestArticle> restored;
        REQUIRE_FALSE(glz::read_json(restored, json));
        REQUIRE(restored[0].view_count.has_value());
        REQUIRE(*restored[0].view_count == 42);
    }

    SECTION("[Glaze] round-trip preserves nullable absent") {
        std::vector<TestArticle> restored;
        REQUIRE_FALSE(glz::read_json(restored, json));
        REQUIRE_FALSE(restored[1].view_count.has_value());
    }

    SECTION("[Glaze] round-trip preserves all scalar fields") {
        std::vector<TestArticle> restored;
        REQUIRE_FALSE(glz::read_json(restored, json));
        REQUIRE(restored.size() == 2);
        REQUIRE(restored[0].id == 10);
        REQUIRE(restored[0].category == "tech");
        REQUIRE(restored[0].author_id == 7);
        REQUIRE(restored[0].title == "With views");
        REQUIRE(restored[0].is_published == true);
        REQUIRE(restored[0].published_at.has_value());
        REQUIRE_FALSE(restored[0].published_at->empty());
        REQUIRE(restored[1].id == 20);
        REQUIRE(restored[1].category == "science");
        REQUIRE(restored[1].is_published == false);
    }
}

TEST_CASE("Glaze vector round-trip - TestItem", "[wrapper][glaze][item]") {

    relais_test::TestItemModel m;
    m.setId(5);
    m.setName("Widget");
    m.setValue(999);
    m.setDescription("A fine widget");
    m.setIsActive(true);
    m.setCreatedAt(trantor::Date::fromDbStringLocal("2025-01-01 00:00:00"));

    std::vector<TestItem> original = {*TestItem::fromModel(m)};

    std::string json;
    REQUIRE_FALSE(glz::write_json(original, json));

    std::vector<TestItem> restored;
    REQUIRE_FALSE(glz::read_json(restored, json));

    REQUIRE(restored.size() == 1);
    REQUIRE(restored[0].id == 5);
    REQUIRE(restored[0].name == "Widget");
    REQUIRE(restored[0].value == 999);
    REQUIRE(restored[0].description == "A fine widget");
    REQUIRE(restored[0].is_active == true);
    REQUIRE_FALSE(restored[0].created_at.empty());
}

TEST_CASE("Glaze vector round-trip - TestOrder (complex)", "[wrapper][glaze][order]") {

    auto order = buildFullTestOrder();

    std::vector<TestOrder> original = {order};

    std::string json;
    REQUIRE_FALSE(glz::write_json(original, json));

    SECTION("[Glaze] round-trip preserves all field types") {
        std::vector<TestOrder> restored;
        REQUIRE_FALSE(glz::read_json(restored, json));
        REQUIRE(restored.size() == 1);
        auto& o = restored[0];
        REQUIRE(o.id == 100);
        REQUIRE(o.user_id == 42);
        REQUIRE(o.amount == 999);
        REQUIRE(o.discount.has_value());
        REQUIRE(*o.discount == 50);
        REQUIRE(o.is_express == true);
        REQUIRE(o.priority == Priority::High);
        REQUIRE(o.label == "rush-order");
        REQUIRE(o.metadata.str == R"({"x":1})");
        REQUIRE(o.address.street == "123 Main St");
        REQUIRE(o.address.geo.metadata.source == "gps");
        REQUIRE(o.history.size() == 2);
        REQUIRE(o.quantities.size() == 3);
        REQUIRE(o.tags.size() == 2);
    }
}

// #############################################################################
//
//  7. Custom JSON field names — glz::meta<Struct> override
//
//  When a shared struct header defines glz::meta<Struct> with custom JSON
//  field names, EntityWrapper automatically detects and uses them for both
//  JSON and BEVE serialization. This ensures the API and BEVE consumers
//  share the same naming contract.
//
//  If no glz::meta<Struct> exists, Mapping::glaze_value is used (member names).
//
// #############################################################################

namespace custom_json_test {

/// A test entity with snake_case C++ members but camelCase JSON names.
/// Represents the shared struct header pattern: struct + glz::meta in one file.
struct Product {
    int64_t id = 0;
    std::string product_name;
    int32_t unit_price = 0;
};

} // namespace custom_json_test

// Custom JSON field names — this would live alongside the struct in a shared header.
// EntityWrapper detects this specialization and uses it instead of Mapping::glaze_value.
template<>
struct glz::meta<custom_json_test::Product> {
    using T = custom_json_test::Product;
    static constexpr auto value = glz::object(
        "id", &T::id,
        "productName", &T::product_name,
        "unitPrice", &T::unit_price
    );
};

namespace custom_json_test {

/// Minimal hand-written mapping for testing.
/// Its glaze_value uses snake_case — this should NOT be used when glz::meta<Product> exists.
struct ProductMapping {
    using Model = relais_test::TestItemModel;
    static constexpr bool read_only = true;

    struct TraitsType {
        using Model = relais_test::TestItemModel;
        enum class Field : uint8_t {};
    };

    template<typename Entity>
    static auto getPrimaryKey(const Entity& e) noexcept { return e.id; }

    template<typename Entity>
    static std::optional<Entity> fromModel(const Model&) { return std::nullopt; }

    template<typename Entity>
    static Model toModel(const Entity&) { return {}; }

    // Fallback: snake_case names (should be overridden by glz::meta<Product>)
    template<typename T>
    static constexpr auto glaze_value = glz::object(
        "id", &T::id,
        "product_name", &T::product_name,
        "unit_price", &T::unit_price
    );
};

using ProductWrapper = jcailloux::drogon::wrapper::EntityWrapper<Product, ProductMapping>;

} // namespace custom_json_test

TEST_CASE("Custom JSON field names via glz::meta<Struct>", "[wrapper][json][custom-names]") {

    custom_json_test::ProductWrapper product;
    product.id = 42;
    product.product_name = "Widget";
    product.unit_price = 999;

    SECTION("[JSON] uses camelCase names from glz::meta<Product>") {
        auto json = product.toJson();
        REQUIRE(json);
        // Must use camelCase from glz::meta<Product>
        REQUIRE(json->find("\"productName\":\"Widget\"") != std::string::npos);
        REQUIRE(json->find("\"unitPrice\":999") != std::string::npos);
        // Must NOT contain snake_case from Mapping::glaze_value
        REQUIRE(json->find("\"product_name\"") == std::string::npos);
        REQUIRE(json->find("\"unit_price\"") == std::string::npos);
    }

    SECTION("[JSON] round-trip preserves all fields") {
        auto json = product.toJson();
        auto restored = custom_json_test::ProductWrapper::fromJson(*json);
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 42);
        REQUIRE(restored->product_name == "Widget");
        REQUIRE(restored->unit_price == 999);
    }

    SECTION("[Binary] BEVE round-trip preserves all fields") {
        auto restored = custom_json_test::ProductWrapper::fromBinary(*product.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->id == 42);
        REQUIRE(restored->product_name == "Widget");
        REQUIRE(restored->unit_price == 999);
    }

    SECTION("[Struct] getPrimaryKey works") {
        REQUIRE(product.getPrimaryKey() == 42);
    }
}

TEST_CASE("ListWrapper items use custom JSON field names", "[wrapper][list][custom-names]") {

    using ProductList = jcailloux::drogon::wrapper::ListWrapper<custom_json_test::ProductWrapper>;

    custom_json_test::ProductWrapper p1;
    p1.id = 1;
    p1.product_name = "Widget";
    p1.unit_price = 100;

    custom_json_test::ProductWrapper p2;
    p2.id = 2;
    p2.product_name = "Gadget";
    p2.unit_price = 200;

    ProductList list;
    list.items = {p1, p2};
    list.total_count = 2;

    SECTION("[List->JSON] items serialized with camelCase names") {
        auto json = list.toJson();
        REQUIRE(json);
        REQUIRE(json->find("\"productName\":\"Widget\"") != std::string::npos);
        REQUIRE(json->find("\"productName\":\"Gadget\"") != std::string::npos);
        REQUIRE(json->find("\"unitPrice\":100") != std::string::npos);
        REQUIRE(json->find("\"product_name\"") == std::string::npos);
    }

    SECTION("[List] BEVE round-trip preserves items") {
        auto restored = ProductList::fromBinary(*list.toBinary());
        REQUIRE(restored.has_value());
        REQUIRE(restored->size() == 2);
        REQUIRE(restored->items[0].product_name == "Widget");
        REQUIRE(restored->items[1].unit_price == 200);
    }
}

TEST_CASE("releaseCaches() frees serialization data while callers retain copies", "[wrapper][cache]") {

    TestUser user;
    user.id = 42;
    user.username = "alice";
    user.email = "alice@example.com";
    user.balance = 1000;
    user.created_at = "2025-01-01T00:00:00Z";

    SECTION("[Entity] callers retain binary data after releaseCaches") {
        auto binary = user.toBinary();
        REQUIRE(binary);
        REQUIRE_FALSE(binary->empty());
        auto size_before = binary->size();

        user.releaseCaches();

        // Caller's shared_ptr still valid
        REQUIRE(binary->size() == size_before);
        // Entity's BEVE cache is gone (once_flag already triggered)
        REQUIRE_FALSE(user.toBinary());
    }

    SECTION("[Entity] callers retain JSON data after releaseCaches") {
        auto json = user.toJson();
        REQUIRE(json);
        REQUIRE(json->find("\"username\":\"alice\"") != std::string::npos);

        user.releaseCaches();

        // Caller's shared_ptr still valid
        REQUIRE(json->find("\"username\":\"alice\"") != std::string::npos);
    }

    SECTION("[List] releaseCaches works on ListWrapper") {
        using ListWrapperUser = jcailloux::drogon::wrapper::ListWrapper<TestUser>;
        ListWrapperUser list;
        list.items = {user};
        list.total_count = 1;

        auto binary = list.toBinary();
        auto json = list.toJson();
        REQUIRE(binary);
        REQUIRE(json);

        list.releaseCaches();

        // Callers' shared_ptrs still valid
        REQUIRE_FALSE(binary->empty());
        REQUIRE(json->find("\"username\":\"alice\"") != std::string::npos);
        // List's caches are gone
        REQUIRE_FALSE(list.toBinary());
        REQUIRE_FALSE(list.toJson());
    }
}

TEST_CASE("Entities without glz::meta<Struct> still use Mapping::glaze_value", "[wrapper][json][custom-names]") {

    // TestUser has NO glz::meta<relais_test::TestUser> specialization,
    // so Mapping::glaze_value (member names) should be used.
    TestUser user;
    user.id = 1;
    user.username = "alice";
    user.email = "alice@test.com";
    user.balance = 100;
    user.created_at = "2025-01-01T00:00:00Z";

    auto json = user.toJson();
    REQUIRE(json);
    REQUIRE(json->find("\"username\":\"alice\"") != std::string::npos);
    REQUIRE(json->find("\"balance\":100") != std::string::npos);
}
