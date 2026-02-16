/**
 * test_base_repository.cpp
 *
 * Tests for BaseRepo (L3 - direct database access, no caching).
 * Uses Uncached configurations that resolve to BaseRepo via Repo<>.
 *
 *   1. TestItem    — CRUD, edge cases, multiple entities, serialization
 *   2. TestUser    — CRUD with different entity structure
 *   3. TestPurchase— FK-constrained entity, cross-entity queries
 *   7. Lists       — uncached list queries (cachedList, cachedListAs pass-through)
 *
 * SECTION naming convention:
 *   [findById]     — read by primary key
 *   [create]       — insert new entity
 *   [update]       — modify existing entity
 *   [remove]       — delete entity
 *   [edge]         — edge cases (nulls, special chars, boundaries)
 *   [multi]        — multiple entities coexistence
 *   [json]         — JSON serialization round-trip
 */

#include <catch2/catch_test_macros.hpp>

#include "fixtures/test_helper.h"
#include "fixtures/TestRepositories.h"

using namespace relais_test;

// #############################################################################
//
//  1. TestItem — basic entity CRUD via UncachedTestItemRepo
//
// #############################################################################

TEST_CASE("BaseRepo<TestItem> - findById", "[integration][db][base][item]") {
    TransactionGuard tx;

    SECTION("[findById] returns entity when it exists") {
        auto id = insertTestItem("Test Item", 42, std::optional<std::string>{"A description"}, true);

        auto result = sync(UncachedTestItemRepo::findById(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id);
        REQUIRE(result->name == "Test Item");
        REQUIRE(result->value == 42);
        REQUIRE(!result->description.empty());
        REQUIRE(result->description == "A description");
        REQUIRE(result->is_active == true);
    }

    SECTION("[findById] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestItemRepo::findById(999999999));

        REQUIRE(result == nullptr);
    }

    SECTION("[findById] returns correct entity among multiple") {
        auto id1 = insertTestItem("First", 1);
        auto id2 = insertTestItem("Second", 2);
        auto id3 = insertTestItem("Third", 3);

        auto result = sync(UncachedTestItemRepo::findById(id2));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id2);
        REQUIRE(result->name == "Second");
        REQUIRE(result->value == 2);
    }
}

TEST_CASE("BaseRepo<TestItem> - create", "[integration][db][base][item]") {
    TransactionGuard tx;

    SECTION("[create] inserts entity and returns with generated id") {
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem("Created Item", 100, "Created via repository", true)));

        REQUIRE(result != nullptr);
        REQUIRE(result->id > 0);
        REQUIRE(result->name == "Created Item");
        REQUIRE(result->description == "Created via repository");
        REQUIRE(result->is_active);
        REQUIRE(result->value == 100);
    }

    SECTION("[create] entity is retrievable after insert") {
        auto created = sync(UncachedTestItemRepo::create(
            makeTestItem("Persistent", 50, "", true)));
        REQUIRE(created != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(created->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == "Persistent");
        REQUIRE(fetched->value == 50);
        REQUIRE(fetched->is_active);
    }

    SECTION("[create] with null optional field") {
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem("No Description", 0, "", true)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->description.empty());
    }
}

TEST_CASE("BaseRepo<TestItem> - update", "[integration][db][base][item]") {
    TransactionGuard tx;

    SECTION("[update] modifies existing entity") {
        auto id = insertTestItem("Original", 10);

        auto fetched = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(fetched);
        REQUIRE(fetched->name == "Original");

        auto success = sync(UncachedTestItemRepo::update(id,
            makeTestItem("Updated", 20, "", true, id)));
        REQUIRE(success == true);

        fetched = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == "Updated");
        REQUIRE(fetched->value == 20);
    }

    SECTION("[update] preserves fields not changed") {
        auto id = insertTestItem("Keep Name", 10, std::optional<std::string>{"Keep Desc"}, true);

        auto original = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(original != nullptr);

        sync(UncachedTestItemRepo::update(id,
            makeTestItem(original->name, 999, original->description, original->is_active, id)));

        auto fetched = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == "Keep Name");
        REQUIRE(fetched->value == 999);
        REQUIRE(!fetched->description.empty());
        REQUIRE(fetched->description == "Keep Desc");
    }
}

TEST_CASE("BaseRepo<TestItem> - remove", "[integration][db][base][item]") {
    TransactionGuard tx;

    SECTION("[remove] deletes existing entity") {
        auto id = insertTestItem("To Delete", 0);

        auto before = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(before != nullptr);

        auto removed = sync(UncachedTestItemRepo::remove(id));
        REQUIRE(removed.has_value());
        REQUIRE(*removed == 1);

        auto after = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(after == nullptr);
    }

    SECTION("[remove] returns 0 rows for non-existent id") {
        auto removed = sync(UncachedTestItemRepo::remove(999999999));
        REQUIRE(removed.has_value());
        REQUIRE(*removed == 0);
    }

    SECTION("[remove] does not affect other entities") {
        auto id1 = insertTestItem("Keep", 1);
        auto id2 = insertTestItem("Delete", 2);
        auto id3 = insertTestItem("Keep Too", 3);

        sync(UncachedTestItemRepo::remove(id2));

        REQUIRE(sync(UncachedTestItemRepo::findById(id1)) != nullptr);
        REQUIRE(sync(UncachedTestItemRepo::findById(id2)) == nullptr);
        REQUIRE(sync(UncachedTestItemRepo::findById(id3)) != nullptr);
    }
}

TEST_CASE("BaseRepo<TestItem> - edge cases", "[integration][db][base][item][edge]") {
    TransactionGuard tx;

    SECTION("[edge] special characters in string fields") {
        std::string specialName = "Test 'quotes\" and <special> chars & more";
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem(specialName, 0, "", true)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name == specialName);
    }

    SECTION("[edge] maximum length name (100 chars)") {
        std::string longName(100, 'X');
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem(longName, 0, "", true)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->name.length() == 100);
    }

    SECTION("[edge] negative numeric value") {
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem("Negative", -12345, "", true)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->value == -12345);
    }

    SECTION("[edge] zero numeric value") {
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem("Zero", 0, "", true)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->value == 0);
    }

    SECTION("[edge] boolean false is preserved") {
        auto result = sync(UncachedTestItemRepo::create(
            makeTestItem("Inactive", 0, "", false)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestItemRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->is_active == false);
    }
}

TEST_CASE("BaseRepo<TestItem> - JSON serialization", "[integration][db][base][item][json]") {
    TransactionGuard tx;

    SECTION("[json] toJson returns valid JSON with all fields") {
        auto id = insertTestItem("Serialization Test", 42, std::optional<std::string>{"desc"}, true);

        auto original = sync(UncachedTestItemRepo::findById(id));

        REQUIRE(original != nullptr);

        auto json = original->toJson();
        REQUIRE(json != nullptr);
        REQUIRE(!json->empty());
        REQUIRE(json->find("Serialization Test") != std::string::npos);
        REQUIRE(json->find("42") != std::string::npos);
        REQUIRE(json->find("desc") != std::string::npos);
    }

    SECTION("[json] toJson with null description") {
        auto id = insertTestItem("No Desc", 0);

        auto entity = sync(UncachedTestItemRepo::findById(id));
        REQUIRE(entity != nullptr);

        auto json = entity->toJson();
        REQUIRE(json != nullptr);
        REQUIRE(!json->empty());
        REQUIRE(json->find("No Desc") != std::string::npos);
    }
}

// #############################################################################
//
//  2. TestUser — different entity structure, CRUD via UncachedTestUserRepo
//
// #############################################################################

TEST_CASE("BaseRepo<TestUser> - findById", "[integration][db][base][user]") {
    TransactionGuard tx;

    SECTION("[findById] returns user when it exists") {
        auto id = insertTestUser("alice", "alice@example.com", 1000);

        auto result = sync(UncachedTestUserRepo::findById(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id);
        REQUIRE(result->username == "alice");
        REQUIRE(result->email == "alice@example.com");
        REQUIRE(result->balance == 1000);
    }

    SECTION("[findById] returns nullptr for non-existent user") {
        auto result = sync(UncachedTestUserRepo::findById(999999999));

        REQUIRE(result == nullptr);
    }
}

TEST_CASE("BaseRepo<TestUser> - create", "[integration][db][base][user]") {
    TransactionGuard tx;

    SECTION("[create] inserts user and returns with generated id") {
        auto result = sync(UncachedTestUserRepo::create(
            makeTestUser("bob", "bob@example.com", 500)));

        REQUIRE(result != nullptr);
        REQUIRE(result->id > 0);
        REQUIRE(result->username == "bob");
        REQUIRE(result->email == "bob@example.com");
        REQUIRE(result->balance == 500);
    }

    SECTION("[create] user is retrievable after insert") {
        auto created = sync(UncachedTestUserRepo::create(
            makeTestUser("carol", "carol@example.com", 0)));
        REQUIRE(created != nullptr);

        auto fetched = sync(UncachedTestUserRepo::findById(created->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->username == "carol");
        REQUIRE(fetched->balance == 0);
    }
}

TEST_CASE("BaseRepo<TestUser> - update", "[integration][db][base][user]") {
    TransactionGuard tx;

    SECTION("[update] modifies user balance") {
        auto id = insertTestUser("alice", "alice@example.com", 100);

        auto original = sync(UncachedTestUserRepo::findById(id));
        REQUIRE(original != nullptr);

        auto success = sync(UncachedTestUserRepo::update(id,
            makeTestUser(original->username,
                           original->email, 999, id)));
        REQUIRE(success == true);

        auto fetched = sync(UncachedTestUserRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->balance == 999);
    }
}

TEST_CASE("BaseRepo<TestUser> - remove", "[integration][db][base][user]") {
    TransactionGuard tx;

    SECTION("[remove] deletes existing user") {
        auto id = insertTestUser("todelete", "del@example.com", 0);

        auto removed = sync(UncachedTestUserRepo::remove(id));
        REQUIRE(removed.has_value());
        REQUIRE(*removed == 1);

        REQUIRE(sync(UncachedTestUserRepo::findById(id)) == nullptr);
    }
}

// #############################################################################
//
//  3. TestPurchase — FK-constrained entity via UncachedTestPurchaseRepo
//
// #############################################################################

TEST_CASE("BaseRepo<TestPurchase> - findById", "[integration][db][base][purchase]") {
    TransactionGuard tx;

    SECTION("[findById] returns purchase when it exists") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        auto id = insertTestPurchase(userId, "Widget", 999, "completed");

        auto result = sync(UncachedTestPurchaseRepo::findById(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id);
        REQUIRE(result->user_id == userId);
        REQUIRE(result->product_name == "Widget");
        REQUIRE(result->amount == 999);
        REQUIRE(result->status == "completed");
    }

    SECTION("[findById] returns nullptr for non-existent purchase") {
        auto result = sync(UncachedTestPurchaseRepo::findById(999999999));

        REQUIRE(result == nullptr);
    }
}

TEST_CASE("BaseRepo<TestPurchase> - create", "[integration][db][base][purchase]") {
    TransactionGuard tx;

    SECTION("[create] inserts purchase with valid FK") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 500);

        auto result = sync(UncachedTestPurchaseRepo::create(
            makeTestPurchase(userId, "Gadget", 250, "pending")));

        REQUIRE(result != nullptr);
        REQUIRE(result->id > 0);
        REQUIRE(result->user_id == userId);
        REQUIRE(result->product_name == "Gadget");
        REQUIRE(result->amount == 250);
        REQUIRE(result->status == "pending");
    }

    SECTION("[create] purchase is retrievable after insert") {
        auto userId = insertTestUser("buyer2", "buyer2@example.com", 100);

        auto created = sync(UncachedTestPurchaseRepo::create(
            makeTestPurchase(userId, "Doohickey", 75, "pending")));
        REQUIRE(created != nullptr);

        auto fetched = sync(UncachedTestPurchaseRepo::findById(created->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->product_name == "Doohickey");
    }
}

TEST_CASE("BaseRepo<TestPurchase> - update", "[integration][db][base][purchase]") {
    TransactionGuard tx;

    SECTION("[update] modifies purchase status and amount") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        auto id = insertTestPurchase(userId, "Widget", 100, "pending");

        auto original = sync(UncachedTestPurchaseRepo::findById(id));
        REQUIRE(original != nullptr);

        auto success = sync(UncachedTestPurchaseRepo::update(id,
            makeTestPurchase(userId, original->product_name, 200, "completed", id)));
        REQUIRE(success == true);

        auto fetched = sync(UncachedTestPurchaseRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->amount == 200);
        REQUIRE(fetched->status == "completed");
    }
}

TEST_CASE("BaseRepo<TestPurchase> - remove", "[integration][db][base][purchase]") {
    TransactionGuard tx;

    SECTION("[remove] deletes purchase without affecting parent user") {
        auto userId = insertTestUser("buyer", "buyer@example.com", 1000);
        auto purchaseId = insertTestPurchase(userId, "Widget", 100);

        auto removed = sync(UncachedTestPurchaseRepo::remove(purchaseId));
        REQUIRE(removed.has_value());
        REQUIRE(*removed == 1);

        REQUIRE(sync(UncachedTestPurchaseRepo::findById(purchaseId)) == nullptr);
        REQUIRE(sync(UncachedTestUserRepo::findById(userId)) != nullptr);
    }
}

TEST_CASE("BaseRepo<TestPurchase> - multiple purchases per user", "[integration][db][base][purchase][multi]") {
    TransactionGuard tx;

    SECTION("[multi] user can have multiple purchases") {
        auto userId = insertTestUser("shopper", "shop@example.com", 5000);
        auto p1 = insertTestPurchase(userId, "Item A", 100, "completed");
        auto p2 = insertTestPurchase(userId, "Item B", 200, "pending");
        auto p3 = insertTestPurchase(userId, "Item C", 300, "completed");

        auto r1 = sync(UncachedTestPurchaseRepo::findById(p1));
        auto r2 = sync(UncachedTestPurchaseRepo::findById(p2));
        auto r3 = sync(UncachedTestPurchaseRepo::findById(p3));

        REQUIRE(r1 != nullptr);
        REQUIRE(r2 != nullptr);
        REQUIRE(r3 != nullptr);

        REQUIRE(r1->product_name == "Item A");
        REQUIRE(r2->product_name == "Item B");
        REQUIRE(r3->product_name == "Item C");

        REQUIRE(r1->user_id == userId);
        REQUIRE(r2->user_id == userId);
        REQUIRE(r3->user_id == userId);
    }

    SECTION("[multi] removing one purchase does not affect others") {
        auto userId = insertTestUser("shopper", "shop@example.com", 5000);
        auto p1 = insertTestPurchase(userId, "Keep A", 100);
        auto p2 = insertTestPurchase(userId, "Delete B", 200);
        auto p3 = insertTestPurchase(userId, "Keep C", 300);

        sync(UncachedTestPurchaseRepo::remove(p2));

        REQUIRE(sync(UncachedTestPurchaseRepo::findById(p1)) != nullptr);
        REQUIRE(sync(UncachedTestPurchaseRepo::findById(p2)) == nullptr);
        REQUIRE(sync(UncachedTestPurchaseRepo::findById(p3)) != nullptr);
    }
}

// #############################################################################
//
//  4. updateBy — partial field update via entity with Traits
//     Uses UncachedTestUserRepo (TestUser entity with Field enum)
//
// #############################################################################

using jcailloux::relais::wrapper::set;
using jcailloux::relais::wrapper::setNull;
using F = TestUserWrapper::Field;

TEST_CASE("BaseRepo - updateBy single field", "[integration][db][base][updateBy]") {
    TransactionGuard tx;

    SECTION("[updateBy] updates only balance, other fields intact") {
        auto id = insertTestUser("alice", "alice@example.com", 100);

        auto result = sync(UncachedTestUserRepo::updateBy(id, set<F::balance>(999)));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 999);
        REQUIRE(result->username == "alice");
        REQUIRE(result->email == "alice@example.com");
    }

    SECTION("[updateBy] updates only username, other fields intact") {
        auto id = insertTestUser("bob", "bob@example.com", 500);

        auto result = sync(UncachedTestUserRepo::updateBy(id,
            set<F::username>(std::string("robert"))));

        REQUIRE(result != nullptr);
        REQUIRE(result->username == "robert");
        REQUIRE(result->email == "bob@example.com");
        REQUIRE(result->balance == 500);
    }
}

TEST_CASE("BaseRepo - updateBy multiple fields", "[integration][db][base][updateBy]") {
    TransactionGuard tx;

    SECTION("[updateBy] updates balance and username together") {
        auto id = insertTestUser("carol", "carol@example.com", 200);

        auto result = sync(UncachedTestUserRepo::updateBy(id,
            set<F::balance>(777),
            set<F::username>(std::string("caroline"))));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 777);
        REQUIRE(result->username == "caroline");
        REQUIRE(result->email == "carol@example.com");
    }

    SECTION("[updateBy] updates all non-PK fields") {
        auto id = insertTestUser("dave", "dave@example.com", 300);

        auto result = sync(UncachedTestUserRepo::updateBy(id,
            set<F::balance>(0),
            set<F::username>(std::string("david")),
            set<F::email>(std::string("david@newdomain.com"))));

        REQUIRE(result != nullptr);
        REQUIRE(result->balance == 0);
        REQUIRE(result->username == "david");
        REQUIRE(result->email == "david@newdomain.com");
    }
}

TEST_CASE("BaseRepo - updateBy returns re-fetched entity", "[integration][db][base][updateBy]") {
    TransactionGuard tx;

    SECTION("[updateBy] returned entity reflects DB state") {
        auto id = insertTestUser("eve", "eve@example.com", 400);

        auto result = sync(UncachedTestUserRepo::updateBy(id, set<F::balance>(123)));
        REQUIRE(result != nullptr);

        // Verify by independent fetch
        auto fetched = sync(UncachedTestUserRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->balance == 123);
        REQUIRE(fetched->balance == result->balance);
    }

    SECTION("[updateBy] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestUserRepo::updateBy(999999999,
            set<F::balance>(999)));

        // updateBy calls mapper.update which may throw or succeed with 0 rows,
        // then re-fetches which returns nullptr
        // updateBy on non-existent ID: either nullptr or exception -> nullptr
        REQUIRE(result == nullptr);
    }
}

// #############################################################################
//
//  5. TestArticle — entity for list scenarios, FK to user
//
// #############################################################################

TEST_CASE("BaseRepo<TestArticle> - findById", "[integration][db][base][article]") {
    TransactionGuard tx;

    SECTION("[findById] returns article when it exists") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id = insertTestArticle("tech", userId, "My Article", 42, true);

        auto result = sync(UncachedTestArticleRepo::findById(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id);
        REQUIRE(result->category == "tech");
        REQUIRE(result->author_id == userId);
        REQUIRE(result->title == "My Article");
        REQUIRE(result->view_count == 42);
        REQUIRE(result->is_published == true);
    }

    SECTION("[findById] returns nullptr for non-existent id") {
        auto result = sync(UncachedTestArticleRepo::findById(999999999));

        REQUIRE(result == nullptr);
    }

    SECTION("[findById] returns correct article among multiple") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id1 = insertTestArticle("tech", userId, "First", 10);
        auto id2 = insertTestArticle("news", userId, "Second", 20, true);
        auto id3 = insertTestArticle("tech", userId, "Third", 30);

        auto result = sync(UncachedTestArticleRepo::findById(id2));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id2);
        REQUIRE(result->title == "Second");
        REQUIRE(result->category == "news");
        REQUIRE(result->is_published == true);
    }
}

TEST_CASE("BaseRepo<TestArticle> - create", "[integration][db][base][article]") {
    TransactionGuard tx;

    SECTION("[create] inserts article and returns with generated id") {
        auto userId = insertTestUser("author", "author@example.com", 0);

        auto result = sync(UncachedTestArticleRepo::create(
            makeTestArticle("science", userId, "Created Article", 0, false)));

        REQUIRE(result != nullptr);
        REQUIRE(result->id > 0);
        REQUIRE(result->category == "science");
        REQUIRE(result->author_id == userId);
        REQUIRE(result->title == "Created Article");
        REQUIRE(result->is_published == false);
    }

    SECTION("[create] article is retrievable after insert") {
        auto userId = insertTestUser("author", "author@example.com", 0);

        auto created = sync(UncachedTestArticleRepo::create(
            makeTestArticle("tech", userId, "Persistent Article", 5, true)));
        REQUIRE(created != nullptr);

        auto fetched = sync(UncachedTestArticleRepo::findById(created->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->title == "Persistent Article");
        REQUIRE(fetched->view_count == 5);
        REQUIRE(fetched->is_published == true);
    }
}

TEST_CASE("BaseRepo<TestArticle> - update", "[integration][db][base][article]") {
    TransactionGuard tx;

    SECTION("[update] modifies existing article") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id = insertTestArticle("tech", userId, "Original Title", 10);

        auto original = sync(UncachedTestArticleRepo::findById(id));
        REQUIRE(original != nullptr);

        auto success = sync(UncachedTestArticleRepo::update(id,
            makeTestArticle(original->category, original->author_id,
                              "Updated Title", 999, true, id)));
        REQUIRE(success == true);

        auto fetched = sync(UncachedTestArticleRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->title == "Updated Title");
        REQUIRE(fetched->view_count == 999);
        REQUIRE(fetched->is_published == true);
    }

    SECTION("[update] preserves fields not changed") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id = insertTestArticle("news", userId, "Keep Title", 50, true);

        auto original = sync(UncachedTestArticleRepo::findById(id));
        REQUIRE(original != nullptr);

        sync(UncachedTestArticleRepo::update(id,
            makeTestArticle(original->category, original->author_id,
                              original->title, 100, original->is_published, id)));

        auto fetched = sync(UncachedTestArticleRepo::findById(id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->title == "Keep Title");
        REQUIRE(fetched->category == "news");
        REQUIRE(fetched->view_count == 100);
        REQUIRE(fetched->is_published == true);
    }
}

TEST_CASE("BaseRepo<TestArticle> - remove", "[integration][db][base][article]") {
    TransactionGuard tx;

    SECTION("[remove] deletes existing article") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id = insertTestArticle("tech", userId, "To Delete", 0);

        auto removed = sync(UncachedTestArticleRepo::remove(id));
        REQUIRE(removed.has_value());
        REQUIRE(*removed == 1);

        REQUIRE(sync(UncachedTestArticleRepo::findById(id)) == nullptr);
    }

    SECTION("[remove] does not affect parent user") {
        auto userId = insertTestUser("author", "author@example.com", 100);
        auto articleId = insertTestArticle("tech", userId, "Article", 0);

        sync(UncachedTestArticleRepo::remove(articleId));

        REQUIRE(sync(UncachedTestArticleRepo::findById(articleId)) == nullptr);
        REQUIRE(sync(UncachedTestUserRepo::findById(userId)) != nullptr);
    }

    SECTION("[remove] does not affect other articles") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id1 = insertTestArticle("tech", userId, "Keep A", 0);
        auto id2 = insertTestArticle("tech", userId, "Delete B", 0);
        auto id3 = insertTestArticle("news", userId, "Keep C", 0);

        sync(UncachedTestArticleRepo::remove(id2));

        REQUIRE(sync(UncachedTestArticleRepo::findById(id1)) != nullptr);
        REQUIRE(sync(UncachedTestArticleRepo::findById(id2)) == nullptr);
        REQUIRE(sync(UncachedTestArticleRepo::findById(id3)) != nullptr);
    }
}

TEST_CASE("BaseRepo<TestArticle> - edge cases", "[integration][db][base][article][edge]") {
    TransactionGuard tx;

    SECTION("[edge] boolean false (is_published) is preserved") {
        auto userId = insertTestUser("author", "author@example.com", 0);

        auto result = sync(UncachedTestArticleRepo::create(
            makeTestArticle("tech", userId, "Unpublished", 0, false)));
        REQUIRE(result != nullptr);

        auto fetched = sync(UncachedTestArticleRepo::findById(result->id));
        REQUIRE(fetched != nullptr);
        REQUIRE(fetched->is_published == false);
    }

    SECTION("[edge] multiple articles per user across categories") {
        auto userId = insertTestUser("prolific", "prolific@example.com", 0);
        auto a1 = insertTestArticle("tech", userId, "Tech 1", 10, true);
        auto a2 = insertTestArticle("news", userId, "News 1", 20, true);
        auto a3 = insertTestArticle("tech", userId, "Tech 2", 30, false);

        auto r1 = sync(UncachedTestArticleRepo::findById(a1));
        auto r2 = sync(UncachedTestArticleRepo::findById(a2));
        auto r3 = sync(UncachedTestArticleRepo::findById(a3));

        REQUIRE(r1 != nullptr);
        REQUIRE(r2 != nullptr);
        REQUIRE(r3 != nullptr);

        REQUIRE(r1->category == "tech");
        REQUIRE(r2->category == "news");
        REQUIRE(r3->category == "tech");

        REQUIRE(r1->author_id == userId);
        REQUIRE(r2->author_id == userId);
        REQUIRE(r3->author_id == userId);
    }
}

TEST_CASE("BaseRepo<TestArticle> - JSON serialization", "[integration][db][base][article][json]") {
    TransactionGuard tx;

    SECTION("[json] toJson returns valid JSON with all fields") {
        auto userId = insertTestUser("author", "author@example.com", 0);
        auto id = insertTestArticle("tech", userId, "JSON Test", 42, true);

        auto original = sync(UncachedTestArticleRepo::findById(id));
        REQUIRE(original != nullptr);

        auto json = original->toJson();
        REQUIRE(json != nullptr);
        REQUIRE(!json->empty());
        REQUIRE(json->find("tech") != std::string::npos);
        REQUIRE(json->find("JSON Test") != std::string::npos);
    }
}

// #############################################################################
//
//  6. Read-only BaseRepo — compile-time write enforcement
//
// #############################################################################

TEST_CASE("BaseRepo - read-only configuration", "[integration][db][base][readonly]") {
    TransactionGuard tx;

    // Compile-time checks
    static_assert(test_config::ReadOnlyUncached.read_only == true);
    static_assert(cfg::Uncached.read_only == false);

    SECTION("[readonly] findById works on read-only repository") {
        auto id = insertTestItem("ReadOnly Test", 42, std::optional<std::string>{"desc"}, true);

        auto result = sync(ReadOnlyTestItemRepo::findById(id));

        REQUIRE(result != nullptr);
        REQUIRE(result->id == id);
        REQUIRE(result->name == "ReadOnly Test");
        REQUIRE(result->value == 42);
        REQUIRE(!result->description.empty());
        REQUIRE(result->description == "desc");
        REQUIRE(result->is_active == true);
    }

    SECTION("[readonly] findById returns nullptr for non-existent id") {
        auto result = sync(ReadOnlyTestItemRepo::findById(999999999));

        REQUIRE(result == nullptr);
    }

    SECTION("[readonly] returns correct entity among multiple") {
        auto id1 = insertTestItem("RO First", 1);
        auto id2 = insertTestItem("RO Second", 2);
        auto id3 = insertTestItem("RO Third", 3);

        auto result = sync(ReadOnlyTestItemRepo::findById(id2));

        REQUIRE(result != nullptr);
        REQUIRE(result->name == "RO Second");
        REQUIRE(result->value == 2);
    }

    // Note: create(), update(), remove() are compile-time errors on read-only repos.
    // They use `requires (!Cfg.read_only)` and will not compile if called.
    // This is verified by the static_assert above.
}

// #############################################################################
//
//  7. List queries — uncached pass-through (cachedList, cachedListAs, etc.)
//
// #############################################################################

namespace {

// Local repos for uncached list testing
namespace uncached_list {

/**
 * Uncached article list repo — uses cachedList pass-through.
 */
class UncachedArticleListRepo : public Repo<TestArticleWrapper, "test:article:list:uncached", cfg::Uncached> {
public:
    static io::Task<std::vector<TestArticleWrapper>> getByCategory(
        const std::string& category, int limit = 10)
    {
        co_return co_await cachedList(
            [category, limit]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY created_at DESC LIMIT $2",
                    category, limit);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            "category", category
        );
    }

    static io::Task<std::vector<TestArticleWrapper>> getByCategoryTracked(
        const std::string& category, int limit = 5, int offset = 0)
    {
        co_return co_await cachedListTracked(
            [category, limit, offset]() -> io::Task<std::vector<TestArticleWrapper>> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY view_count DESC LIMIT $2 OFFSET $3",
                    category, limit, offset);
                std::vector<TestArticleWrapper> entities;
                for (size_t i = 0; i < result.rows(); ++i) {
                    if (auto e = entity::generated::TestArticleMapping::fromRow<TestArticleWrapper>(result[i]))
                        entities.push_back(std::move(*e));
                }
                co_return entities;
            },
            limit, offset,
            "category", category
        );
    }

    // Expose invalidation methods for testing (no-ops at Base level)
    static io::Task<size_t> invalidateCategoryGroup(const std::string& category) {
        co_return co_await invalidateListGroup("category", category);
    }

    static io::Task<size_t> invalidateCategorySelective(
        const std::string& category, int64_t sort_val)
    {
        co_return co_await invalidateListGroupSelective(sort_val, "category", category);
    }

    static io::Task<size_t> invalidateCategorySelectiveUpdate(
        const std::string& category, int64_t old_val, int64_t new_val)
    {
        co_return co_await invalidateListGroupSelectiveUpdate(old_val, new_val, "category", category);
    }
};

/**
 * Uncached article list repo — uses cachedListAs pass-through (typed list entity).
 */
class UncachedArticleListAsRepo : public Repo<TestArticleWrapper, "test:article:as:list:uncached", cfg::Uncached> {
public:
    static io::Task<TestArticleList> getByCategory(
        const std::string& category, int limit = 10)
    {
        co_return co_await cachedListAs<TestArticleList>(
            [category, limit]() -> io::Task<TestArticleList> {
                auto result = co_await jcailloux::relais::DbProvider::queryArgs(
                    "SELECT id, category, author_id, title, view_count, is_published, published_at, created_at "
                    "FROM relais_test_articles WHERE category = $1 ORDER BY created_at DESC LIMIT $2",
                    category, limit);
                co_return TestArticleList::fromRows(result);
            },
            "category", category
        );
    }
};

} // namespace uncached_list
} // anonymous namespace

TEST_CASE("BaseRepo - uncached list queries (JSON)", "[integration][db][base][list]") {
    TransactionGuard tx;
    using Repo = uncached_list::UncachedArticleListRepo;

    SECTION("[list] query returns articles from database") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10, true);
        insertTestArticle("tech", userId, "Tech 2", 20, true);
        insertTestArticle("news", userId, "News 1", 30, true);

        auto result = sync(Repo::getByCategory("tech"));

        REQUIRE(result.size() == 2);
        REQUIRE(result[0].title == "Tech 2");
        REQUIRE(result[1].title == "Tech 1");
    }

    SECTION("[list] no caching — new data visible immediately") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("fresh_cat", userId, "Article 1", 10, true);

        auto result1 = sync(Repo::getByCategory("fresh_cat"));
        REQUIRE(result1.size() == 1);

        // Insert another article directly in DB
        insertTestArticle("fresh_cat", userId, "Article 2", 20, true);

        // Second query — no cache, should see the new article immediately
        auto result2 = sync(Repo::getByCategory("fresh_cat"));
        REQUIRE(result2.size() == 2);
    }

    SECTION("[list] different categories return independent results") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tech", userId, "Tech 1", 10, true);
        insertTestArticle("news", userId, "News 1", 20, true);

        auto tech = sync(Repo::getByCategory("tech"));
        auto news = sync(Repo::getByCategory("news"));

        REQUIRE(tech.size() == 1);
        REQUIRE(news.size() == 1);
        REQUIRE(tech[0].category == "tech");
        REQUIRE(news[0].category == "news");
    }

    SECTION("[list] empty category returns empty list") {
        auto result = sync(Repo::getByCategory("nonexistent"));
        REQUIRE(result.empty());
    }

    SECTION("[list] limit is respected") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 0; i < 5; ++i) {
            insertTestArticle("many", userId, "Art " + std::to_string(i), i * 10, true);
        }

        auto result = sync(Repo::getByCategory("many", 3));
        REQUIRE(result.size() == 3);
    }
}

TEST_CASE("BaseRepo - uncached tracked list queries", "[integration][db][base][list-tracked]") {
    TransactionGuard tx;
    using Repo = uncached_list::UncachedArticleListRepo;

    SECTION("[list-tracked] paginated query returns correct page") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        for (int i = 1; i <= 10; ++i) {
            insertTestArticle("paged", userId, "Art " + std::to_string(i), i * 10, true);
        }

        // Page 0: top 5 by view_count DESC → view_count 100,90,80,70,60
        auto page0 = sync(Repo::getByCategoryTracked("paged", 5, 0));
        REQUIRE(page0.size() == 5);
        REQUIRE(page0[0].view_count == 100);
        REQUIRE(page0[4].view_count == 60);

        // Page 1: next 5 → view_count 50,40,30,20,10
        auto page1 = sync(Repo::getByCategoryTracked("paged", 5, 5));
        REQUIRE(page1.size() == 5);
        REQUIRE(page1[0].view_count == 50);
        REQUIRE(page1[4].view_count == 10);
    }

    SECTION("[list-tracked] no caching — new data visible on re-query") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("tracked_cat", userId, "Art 1", 50, true);
        insertTestArticle("tracked_cat", userId, "Art 2", 40, true);

        auto result1 = sync(Repo::getByCategoryTracked("tracked_cat", 5, 0));
        REQUIRE(result1.size() == 2);

        // Insert higher view_count article
        insertTestArticle("tracked_cat", userId, "Art 3", 60, true);

        auto result2 = sync(Repo::getByCategoryTracked("tracked_cat", 5, 0));
        REQUIRE(result2.size() == 3);
        REQUIRE(result2[0].view_count == 60);
    }
}

TEST_CASE("BaseRepo - uncached list invalidation (no-ops)", "[integration][db][base][list-inv]") {
    TransactionGuard tx;
    using Repo = uncached_list::UncachedArticleListRepo;

    SECTION("[list-inv] invalidateListGroup returns 0 (no-op)") {
        auto count = sync(Repo::invalidateCategoryGroup("tech"));
        REQUIRE(count == 0);
    }

    SECTION("[list-inv] invalidateListGroupSelective returns 0 (no-op)") {
        auto count = sync(Repo::invalidateCategorySelective("tech", 42));
        REQUIRE(count == 0);
    }

    SECTION("[list-inv] invalidateListGroupSelectiveUpdate returns 0 (no-op)") {
        auto count = sync(Repo::invalidateCategorySelectiveUpdate("tech", 42, 99));
        REQUIRE(count == 0);
    }

    SECTION("[list-inv] invalidateListGroupByKey returns 0 (no-op)") {
        auto groupKey = Repo::makeGroupKey("category", "tech");
        auto count = sync(Repo::invalidateListGroupByKey(groupKey, 42));
        REQUIRE(count == 0);
    }
}

TEST_CASE("BaseRepo - uncached list queries (listAs)", "[integration][db][base][list-as]") {
    TransactionGuard tx;
    using Repo = uncached_list::UncachedArticleListAsRepo;

    SECTION("[list-as] query returns list entity") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("as_cat", userId, "Article 1", 10, true);
        insertTestArticle("as_cat", userId, "Article 2", 20, true);

        auto result = sync(Repo::getByCategory("as_cat"));

        REQUIRE(result.size() == 2);
        REQUIRE_FALSE(result.empty());
    }

    SECTION("[list-as] no caching — new data visible immediately") {
        auto userId = insertTestUser("author", "author@test.com", 0);
        insertTestArticle("as_fresh", userId, "Cached", 10, true);

        auto result1 = sync(Repo::getByCategory("as_fresh"));
        REQUIRE(result1.size() == 1);

        // Insert another article
        insertTestArticle("as_fresh", userId, "New", 20, true);

        // No cache — should see 2 articles immediately
        auto result2 = sync(Repo::getByCategory("as_fresh"));
        REQUIRE(result2.size() == 2);
    }

    SECTION("[list-as] empty category returns empty list") {
        auto result = sync(Repo::getByCategory("nonexistent"));
        REQUIRE(result.empty());
    }
}
