package util_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	container "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/core"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

func TestUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg/Util Mongo Suite")
}

//nolint:govet // Entity for testing
type User struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Seq       int                `bson:"seq"`
	Name      string             `bson:"name"`
	UserName  string             `bson:"user_name"`
	Age       uint               `bson:"age"`
	CreatedAt time.Time          `bson:"created_at"`
}

var _ = Describe("MongoQueryFunc Utilities", Ordered, func() {
	var (
		mongodbContainer *container.MongoDBContainer
		mongoHost        string
		mongoPort        int
		baseQuery        *MongoQuery
		db               *mongo.Database
	)

	BeforeAll(func() {
		ctx := context.Background()
		var err error
		mongodbContainer, err = container.Run(
			ctx,
			"mongo:latest",
			testcontainers.WithLogger(GetLogger()),
		)
		Expect(err).NotTo(HaveOccurred())

		ip, err := mongodbContainer.Host(ctx)
		Expect(err).NotTo(HaveOccurred())
		port, err := mongodbContainer.MappedPort(ctx, "27017")
		Expect(err).NotTo(HaveOccurred())

		mongoHost = ip
		mongoPort = int(port.Num())
	})

	AfterAll(func() {
		if mongodbContainer != nil {
			testcontainers.CleanupContainer(GinkgoTB(), mongodbContainer)
		}
	})

	BeforeEach(func() {
		var langErr Error
		db, langErr = NewMongoClientBuilder().
			WithHost(mongoHost).
			WithPort(mongoPort).
			BuildDatabase("rays_mongo_util_test")
		Expect(langErr).NotTo(HaveOccurred())

		ctx := context.Background()
		Expect(db.Collection("users").Drop(ctx)).NotTo(HaveOccurred())
		Expect(db.Collection("users_archive").Drop(ctx)).NotTo(HaveOccurred())

		docs := []any{
			User{Seq: 1, Name: "Alice", UserName: "alice", Age: 20},
			User{Seq: 2, Name: "Bob", UserName: "bob", Age: 30},
			User{Seq: 3, Name: "Charlie", UserName: "charlie", Age: 40},
			User{Seq: 4, Name: "Alice", UserName: "alice2", Age: 25},
		}
		_, err := db.Collection("users").InsertMany(ctx, docs)
		Expect(err).NotTo(HaveOccurred())

		_, err = db.Collection("users_archive").InsertOne(ctx, User{Seq: 99, Name: "Archived", UserName: "archived", Age: 60})
		Expect(err).NotTo(HaveOccurred())

		baseQuery = NewMongoQuery(ctx, db, "users")
	})

	AfterEach(func() {
		if db != nil {
			Expect(db.Client().Disconnect(context.Background())).NotTo(HaveOccurred())
		}
	})

	It("paginates results correctly", func() {
		var page1 []User
		Expect(baseQuery.Scopes(Paginate(1, 2), OrderByAsc("seq")).Find(&page1)).NotTo(HaveOccurred())
		Expect(page1).To(HaveLen(2))
		Expect(page1[0].UserName).To(Equal("alice"))
		Expect(page1[1].UserName).To(Equal("bob"))

		var page2 []User
		Expect(baseQuery.Scopes(Paginate(2, 2), OrderByAsc("seq")).Find(&page2)).NotTo(HaveOccurred())
		Expect(page2).To(HaveLen(2))
		Expect(page2[0].UserName).To(Equal("charlie"))
		Expect(page2[1].UserName).To(Equal("alice2"))

		var pageDefault []User
		Expect(baseQuery.Scopes(Paginate(0, 0), OrderByAsc("seq")).Find(&pageDefault)).NotTo(HaveOccurred())
		Expect(pageDefault).To(HaveLen(4))
	})

	It("supports concurrent Paginate usage without race conditions", func() {
		fn := Paginate(0, 0)
		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				var users []User
				Expect(baseQuery.Scopes(fn).Find(&users)).NotTo(HaveOccurred())
				Expect(users).To(HaveLen(4))
			}()
		}
		wg.Wait()
	})

	It("returns an error on pagination offset overflow without mutating baseQuery", func() {
		var users []User
		err := baseQuery.Scopes(Paginate(math.MaxInt, 10)).Find(&users)
		Expect(err).To(HaveOccurred())

		var langErr Error
		Expect(errors.As(err, &langErr)).To(BeTrue())
		Expect(langErr.Name()).To(Equal("PaginationOverflowError"))
		Expect(langErr.Message()).To(Equal("pagination offset overflow"))

		Expect(baseQuery.Error()).ToNot(HaveOccurred())
		Expect(baseQuery.Find(&users)).NotTo(HaveOccurred())
		Expect(users).To(HaveLen(4))
	})

	It("limits results and enforces zero rows with WithLimit(0)", func() {
		var users []User
		Expect(baseQuery.Scopes(Limit(2)).Find(&users)).NotTo(HaveOccurred())
		Expect(users).To(HaveLen(2))

		var zeroUsers []User
		Expect(baseQuery.Scopes(WithLimit(10), WithLimit(0)).Find(&zeroUsers)).NotTo(HaveOccurred())
		Expect(zeroUsers).To(BeEmpty())

		var overrideZero []User
		Expect(baseQuery.Scopes(WithLimit(0), WithLimit(2)).Find(&overrideZero)).NotTo(HaveOccurred())
		Expect(overrideZero).To(HaveLen(2))
	})

	It("skips results and cancels prior skip with WithOffset(0)", func() {
		var users []User
		Expect(baseQuery.Scopes(WithOffset(2), OrderByAsc("seq")).Find(&users)).NotTo(HaveOccurred())
		Expect(users).To(HaveLen(2))
		Expect(users[0].UserName).To(Equal("charlie"))

		var resetUsers []User
		Expect(baseQuery.Scopes(WithSkip(2), WithOffset(0), OrderByAsc("seq")).Find(&resetUsers)).NotTo(HaveOccurred())
		Expect(resetUsers).To(HaveLen(4))
		Expect(resetUsers[0].UserName).To(Equal("alice"))
	})

	It("orders results with OrderByAsc, OrderByDesc, WithSort, and WithOrder", func() {
		var asc []User
		Expect(baseQuery.Scopes(OrderByAsc("age")).Find(&asc)).NotTo(HaveOccurred())
		Expect(asc[0].Age).To(Equal(uint(20)))

		var desc []User
		Expect(baseQuery.Scopes(OrderByDesc("age")).Find(&desc)).NotTo(HaveOccurred())
		Expect(desc[0].Age).To(Equal(uint(40)))

		var custom []User
		Expect(baseQuery.Scopes(WithSort(bson.D{{Key: "age", Value: -1}, {Key: "user_name", Value: 1}})).Find(&custom)).NotTo(HaveOccurred())
		Expect(custom[0].Age).To(Equal(uint(40)))

		var ordered []User
		Expect(baseQuery.Scopes(WithOrder(bson.D{{Key: "seq", Value: -1}})).Find(&ordered)).NotTo(HaveOccurred())
		Expect(ordered[0].Seq).To(Equal(4))
	})

	It("projects fields with WithSelect, WithOmit, and WithProjection", func() {
		var selected []User
		Expect(baseQuery.Scopes(WithSelect("name"), OrderByAsc("seq")).Find(&selected)).NotTo(HaveOccurred())
		Expect(selected[0].Name).NotTo(BeEmpty())
		Expect(selected[0].Age).To(Equal(uint(0)))

		var omitted []User
		Expect(baseQuery.Scopes(WithOmit("age"), OrderByAsc("seq")).Find(&omitted)).NotTo(HaveOccurred())
		Expect(omitted[0].Name).NotTo(BeEmpty())
		Expect(omitted[0].Age).To(Equal(uint(0)))

		var proj []User
		Expect(baseQuery.Scopes(WithProjection(bson.M{"user_name": 1}), OrderByAsc("seq")).Find(&proj)).NotTo(HaveOccurred())
		Expect(proj[0].UserName).To(Equal("alice"))
		Expect(proj[0].Name).To(BeEmpty())
	})

	It("filters with WithWhere, WithFilter, WithOr, and overrides collection with WithCollection", func() {
		var users []User
		Expect(baseQuery.Scopes(
			WithWhere(bson.M{"age": 20}),
			WithOr(bson.M{"age": 30}),
		).Find(&users)).NotTo(HaveOccurred())
		Expect(users).To(HaveLen(2))

		var filtered []User
		Expect(baseQuery.Scopes(WithFilter(bson.M{"user_name": "bob"})).Find(&filtered)).NotTo(HaveOccurred())
		Expect(filtered).To(HaveLen(1))

		var archived []User
		Expect(baseQuery.Scopes(WithCollection("users_archive")).Find(&archived)).NotTo(HaveOccurred())
		Expect(archived).To(HaveLen(1))
		Expect(archived[0].UserName).To(Equal("archived"))
	})

	It("supports WithHint, WithCollation, WithQueryContext, WithMaxTime, and Combine", func() {
		collation := &options.Collation{Locale: "en", Strength: 2}
		combined := Combine(
			OrderByAsc("age"),
			Limit(2),
			WithHint(bson.D{{Key: "_id", Value: 1}}),
			WithCollation(collation),
			WithQueryContext(context.Background()),
			WithMaxTime(5*time.Second),
		)
		var users []User
		Expect(baseQuery.Scopes(WithWhere(bson.M{"name": "alice"}), combined).Find(&users)).NotTo(HaveOccurred())
		Expect(users).To(HaveLen(2))
		Expect(users[0].Age).To(Equal(uint(20)))
		Expect(users[1].Age).To(Equal(uint(25)))
	})
})
