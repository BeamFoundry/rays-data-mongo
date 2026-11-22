package core_test

import (
	"context"
	"fmt"
	"reflect"
	"runtime/pprof"
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

	_ "github.com/BeamFoundry/rays-data-mongo"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/core"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/testing"
	. "github.com/BeamFoundry/rays/pkg/util/stream"
)

func TestMongo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg/Core Mongo Suite")
}

type Address struct {
	City    string `bson:"city"`
	Country string `bson:"country"`
}

type Animal struct {
	Name   string             `bson:"name"`
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	UserID primitive.ObjectID `bson:"user_id"`
}

func (Animal) CollectionName() string {
	return "custom_animals"
}

type AnimalRepository struct {
	MongoRepository[Animal, primitive.ObjectID]
}

type User struct {
	CreatedAt time.Time          `bson:"created_at"`
	Email     *string            `bson:"email,omitempty"`
	Address   Address            `bson:"address"`
	Name      string             `bson:"name"`
	UserName  string             `bson:"user_name"`
	Age       uint               `bson:"age"`
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Active    bool               `bson:"active"`
}

type Foo struct{}

type UserRepositoryInterface interface {
	MongoRepositoryInterface[User, primitive.ObjectID]
	FindByName(string) *User
	FindByUserName(string) *User
	FindWithErrorByUserName(string) (*User, Error)
	FindAllByName(string) []User
	FindStreamByName(string) Stream[User]
	StreamAllByName(string) Stream[User]
	FindStreamWithErrorByName(string) (Stream[User], Error)
	FindOneByName(string) Optional[*User]
	FindAllByAgeIn([]uint) []User
	FindAllByUserNameAndAge(string, uint) []User
	FindAllByAgeBetweenOrderByAge(uint, uint) []User
	FindAllByAgeBetweenOrderByAgeAsc(uint, uint) []User
	FindAllByAgeBetweenOrderByAgeDesc(uint, uint) []User
	FindByAddressCity(string) []User
	CountByName(string) (int64, Error)
	ExistsByUserName(string) (bool, Error)
	DeleteByUserName(string) (int64, Error)
}

var _ UserRepositoryInterface = &UserRepository{}

type UserRepository struct {
	findByName                              func(string) *User
	findByUserName                          func(string) *User
	findWithErrorByUserName                 func(string) (*User, Error)
	findAllByName                           func(string) []User
	findStreamByName                        func(string) Stream[User]
	streamAllByName                         func(string) Stream[User]
	findStreamWithErrorByName               func(string) (Stream[User], Error)
	findOneByName                           func(string) Optional[*User]
	findAllByAgeIn                          func([]uint) []User
	findAllByUserNameAndAge                 func(string, uint) []User
	findAllByAgeBetweenOrderByAge           func(uint, uint) []User
	findAllByAgeBetweenOrderByAgeAsc        func(uint, uint) []User
	findAllByAgeBetweenOrderByAgeDesc       func(uint, uint) []User
	findAllByAgeBetweenOrderByNameAscAgeAsc func(uint, uint) []User
	findByAddressCity                       func(string) []User
	countByName                             func(string) (int64, Error)
	existsByUserName                        func(string) (bool, Error)
	deleteByUserName                        func(string) (int64, Error)
	findByNameNot                           func(string) []User
	findByEmailIsNull                       func() []User
	findByEmailIsNotNull                    func() []User
	findByNameOrName                        func(string, string) []User
	findByNameLike                          func(string) []User
	findByNameNotLike                       func(string) []User
	findByNameStartingWith                  func(string) []User
	findByNameEndingWith                    func(string) []User
	findByNameContaining                    func(string) []User
	findByActiveTrue                        func() []User
	findByActiveFalse                       func() []User
	findAllByAgeBetween                     func(uint, uint, ...MongoQueryFunc) []User
	queryAllByAgeBetween                    func(uint, uint) *MongoQuery
	findAllByAgeNotIn                       func([]uint) []User
	findAllByAgeGreaterThan                 func(uint) []User
	findAllByAgeGreaterThanOrEqual          func(uint) []User
	findAllByAgeLessThan                    func(uint) []User
	findAllByAgeLessThanOrEqual             func(uint) []User
	detectAllByName                         func(string) []User
	findBadName                             func(string) []User
	findBadByNameIsHave                     func(string) []User
	findBadByNameAndAge                     func(string) []User
	findBadByFunction                       func(string) *User
	findBadPointerByName                    func(string) *Foo
	findBadStructByName                     func(string) Foo
	findBadSliceByName                      func(string) []Foo
	findBadStreamByName                     func(string) Stream[Foo]
	findBadElemByName                       func(string) string
	findByNameOrderByFoo                    func(string) []User
	findByFoo                               func(string) *User
	findBadByAge                            func([]uint) []User
	findBadByAgeIn                          func([]string) []User
	findBadTypeByName                       func(uint) *User
	countBadReturnByName                    func(string) string
	existsBadReturnByName                   func(string) int64
	MongoRepository[User, primitive.ObjectID]
}

func (this *UserRepository) PostConstruct() Error {
	// Verify that PostConstruct initializes the parser function before it can be called
	_ = this.findByName("preflight_init_check")
	_, err := this.EnsureIndexes(context.Background(), mongo.IndexModel{
		Keys: bson.D{{Key: "user_name", Value: 1}},
	})
	return err
}

func (this *UserRepository) FindByName(name string) *User {
	return this.findByName(name)
}

func (this *UserRepository) FindByUserName(name string) *User {
	return this.findByUserName(name)
}

func (this *UserRepository) FindWithErrorByUserName(name string) (*User, Error) {
	return this.findWithErrorByUserName(name)
}

func (this *UserRepository) FindAllByName(name string) []User {
	return this.findAllByName(name)
}

func (this *UserRepository) FindStreamByName(name string) Stream[User] {
	return this.findStreamByName(name)
}

func (this *UserRepository) StreamAllByName(name string) Stream[User] {
	return this.streamAllByName(name)
}

func (this *UserRepository) FindStreamWithErrorByName(name string) (Stream[User], Error) {
	return this.findStreamWithErrorByName(name)
}

func (this *UserRepository) FindOneByName(name string) Optional[*User] {
	return this.findOneByName(name)
}

func (this *UserRepository) FindAllByAgeIn(age []uint) []User {
	return this.findAllByAgeIn(age)
}

func (this *UserRepository) FindAllByUserNameAndAge(name string, age uint) []User {
	return this.findAllByUserNameAndAge(name, age)
}

func (this *UserRepository) FindAllByAgeBetweenOrderByAge(start, end uint) []User {
	return this.findAllByAgeBetweenOrderByAge(start, end)
}

func (this *UserRepository) FindAllByAgeBetweenOrderByAgeAsc(start, end uint) []User {
	return this.findAllByAgeBetweenOrderByAgeAsc(start, end)
}

func (this *UserRepository) FindAllByAgeBetweenOrderByAgeDesc(start, end uint) []User {
	return this.findAllByAgeBetweenOrderByAgeDesc(start, end)
}

func (this *UserRepository) FindByAddressCity(city string) []User {
	return this.findByAddressCity(city)
}

func (this *UserRepository) CountByName(name string) (int64, Error) {
	return this.countByName(name)
}

func (this *UserRepository) ExistsByUserName(userName string) (bool, Error) {
	return this.existsByUserName(userName)
}

func (this *UserRepository) DeleteByUserName(userName string) (int64, Error) {
	return this.deleteByUserName(userName)
}

type TestMongoEndpoint struct {
	Host string
	Port int
}

type TestMongoConfig struct {
	Configuration
	logSink  options.LogSink              `@:"Inject"`
	endpoint Optional[*TestMongoEndpoint] `@:"Inject"`
}

func (t *TestMongoConfig) Client(
	_ struct {
		ConditionalOnMissingBeam
	},
) (*mongo.Client, Error) {
	ep, ok := t.endpoint.Get()
	if !ok {
		return nil, NewError("MongoConfigurationError", "No TestMongoEndpoint provided")
	}
	return NewMongoClientBuilder().
		WithHost(ep.Host).
		WithPort(ep.Port).
		WithDirectConnection(true).
		WithLogSink(t.logSink).
		WithOptions(func(opts *options.ClientOptions) {
			opts.SetAppName("rays-data-mongo-test")
		}).
		Build()
}

func (t *TestMongoConfig) Database(
	that struct {
		ConditionalOnMissingBeam
		client *mongo.Client `@:"Inject"`
	},
) *mongo.Database {
	return that.client.Database("rays_mongo_test")
}

type UserService struct {
	Service
	txManager      TransactionManager      `@:"Inject"`
	userRepository UserRepositoryInterface `@:"Inject"`
}

func (this *UserService) TransactionalChangeRollback(userID primitive.ObjectID) {
	var writeCompleted bool
	var originalName string
	this.txManager.SetContext(context.Background())
	defer this.txManager.ClearContext()

	txErr := this.txManager.Transactional(func() Error {
		user, ok := this.userRepository.FindById(userID).Get()
		Expect(ok).To(BeTrue())
		originalName = user.Name
		user.Name = "bar"
		_, err := this.userRepository.Save(user)
		Expect(err).NotTo(HaveOccurred())
		writeCompleted = true
		reRead, ok := this.userRepository.FindById(userID).Get()
		Expect(ok).To(BeTrue())
		Expect(reRead.Name).To(Equal("bar"))
		return NewError("TransactionalError", "Rolling back")
	})
	Expect(txErr).To(HaveOccurred())

	Expect(writeCompleted).To(BeTrue())
	u := this.userRepository.FindByName("bar")
	Expect(u).To(BeNil())
	original, ok := this.userRepository.FindById(userID).Get()
	Expect(ok).To(BeTrue())
	Expect(original.Name).To(Equal(originalName))
}

func (this *UserService) TransactionalChangeRollbackPprof(userID primitive.ObjectID) {
	ctx := context.Background()
	labels := pprof.Labels("worker", "tx-pprof-test")
	writeCompleted := false
	var originalName string

	pprof.Do(ctx, labels, func(pprofCtx context.Context) {
		this.txManager.SetContext(pprofCtx)
		defer this.txManager.ClearContext()

		txErr := this.txManager.Transactional(func() Error {
			user, ok := this.userRepository.FindById(userID).Get()
			Expect(ok).To(BeTrue())
			originalName = user.Name
			user.Name = "bar-pprof"
			_, err := this.userRepository.Save(user)
			Expect(err).NotTo(HaveOccurred())
			writeCompleted = true
			reRead, ok := this.userRepository.FindById(userID).Get()
			Expect(ok).To(BeTrue())
			Expect(reRead.Name).To(Equal("bar-pprof"))
			return NewError("TransactionalError", "Rolling back inside pprof")
		})
		Expect(txErr).To(HaveOccurred())
	})

	Expect(writeCompleted).To(BeTrue())
	u := this.userRepository.FindByName("bar-pprof")
	Expect(u).To(BeNil())
	original, ok := this.userRepository.FindById(userID).Get()
	Expect(ok).To(BeTrue())
	Expect(original.Name).To(Equal(originalName))
}

func (this *UserService) TransactionalRepositorySetErrorRollback(userID primitive.ObjectID) {
	this.txManager.SetContext(context.Background())
	defer this.txManager.ClearContext()

	var originalName string
	txErr := this.txManager.Transactional(func() Error {
		user, ok := this.userRepository.FindById(userID).Get()
		Expect(ok).To(BeTrue())
		originalName = user.Name
		user.Name = "bar-seterror"
		_, err := this.userRepository.Save(user)
		Expect(err).NotTo(HaveOccurred())

		// Trigger a repository error via SetError and ignore it in the callback return
		_, findErr := this.userRepository.FindWithErrorByUserName("non_existent_user")
		Expect(findErr).To(HaveOccurred())
		return nil
	})
	Expect(txErr).To(HaveOccurred())

	u := this.userRepository.FindByName("bar-seterror")
	Expect(u).To(BeNil())
	original, ok := this.userRepository.FindById(userID).Get()
	Expect(ok).To(BeTrue())
	Expect(original.Name).To(Equal(originalName))
}

func (this *UserService) TransactionalChangeCommit(userID primitive.ObjectID) {
	this.txManager.SetContext(context.Background())
	defer this.txManager.ClearContext()

	txErr := this.txManager.Transactional(func() Error {
		user, ok := this.userRepository.FindById(userID).Get()
		Expect(ok).To(BeTrue())
		user.Name = "bar"
		_, err := this.userRepository.Save(user)
		Expect(err).NotTo(HaveOccurred())
		return nil
	})
	Expect(txErr).NotTo(HaveOccurred())

	u := this.userRepository.FindByName("bar")
	Expect(u).NotTo(BeNil())
	Expect(u.Name).To(Equal("bar"))
}

func (this *UserService) ErrorAfterRollback(userID primitive.ObjectID) {
	this.txManager.SetContext(context.Background())
	defer this.txManager.ClearContext()

	Expect(this.txManager.Start()).NotTo(HaveOccurred())
	defer this.txManager.End()

	user, ok := this.userRepository.FindById(userID).Get()
	Expect(ok).To(BeTrue())
	Expect(this.txManager.Rollback()).NotTo(HaveOccurred())

	// After AbortTransaction, operations using the aborted session fail
	Expect(this.userRepository.Delete(user)).To(HaveOccurred())
	Expect(this.userRepository.DeleteById(userID)).To(HaveOccurred())
	_, err := this.userRepository.Save(user)
	Expect(err).To(HaveOccurred())
	_, err = this.userRepository.SaveAll([]*User{user})
	Expect(err).To(HaveOccurred())
}

var _ = Describe("Rays Data Mongo", Ordered, func() {
	var (
		mongodbContainer *container.MongoDBContainer
		mongoHost        string
		mongoPort        int
	)

	BeforeAll(func() {
		ctx := context.Background()
		var err error
		mongodbContainer, err = container.Run(
			ctx,
			"mongo:8.2.7",
			container.WithReplicaSet("rs0"),
			testcontainers.WithLogger(GetLogger()),
		)
		Expect(err).NotTo(HaveOccurred())

		ip, err := mongodbContainer.Host(ctx)
		Expect(err).NotTo(HaveOccurred())
		port, err := mongodbContainer.MappedPort(ctx, "27017")
		Expect(err).NotTo(HaveOccurred())

		mongoHost = ip
		mongoPort = int(port.Num())
		fmt.Fprintf(GinkgoWriter, "Mongo Container host/port : %s:%d\n", mongoHost, mongoPort)
	})

	AfterAll(func() {
		if mongodbContainer != nil {
			testcontainers.CleanupContainer(GinkgoTB(), mongodbContainer)
		}
	})

	Context("AutoConfiguration and Repositories", func() {
		var (
			userRepository   *UserRepository
			animalRepository *AnimalRepository
			userService      *UserService
			dorothy          *User
		)

		BeforeEach(func() {
			userRepository = &UserRepository{}
			animalRepository = &AnimalRepository{}
			userService = &UserService{}

			AddBeamFor(&TestMongoConfig{})
			AddBeamFor(&TestMongoEndpoint{Host: mongoHost, Port: mongoPort})
			AddBeamFor(userRepository)
			AddBeamFor(animalRepository)
			AddBeamFor(userService)
			GetContainer().EnableScanning()

			Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

			ctx := context.Background()
			Expect(userRepository.Collection().Drop(ctx)).NotTo(HaveOccurred())
			Expect(animalRepository.Collection().Drop(ctx)).NotTo(HaveOccurred())
			// Re-ensure indexes after dropping collection
			_, err := userRepository.EnsureIndexes(ctx, mongo.IndexModel{
				Keys: bson.D{{Key: "user_name", Value: 1}},
			})
			Expect(err).NotTo(HaveOccurred())

			email := "dorothy@oz.example"
			dorothy, err = userRepository.Save(&User{
				Name:      "Dorothy",
				UserName:  "dorothy",
				Age:       25,
				Active:    true,
				Email:     &email,
				Address:   Address{City: "Emerald City", Country: "Oz"},
				CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(dorothy).NotTo(BeNil())
			Expect(dorothy.ID.IsZero()).To(BeFalse())

			animal, err := animalRepository.Save(&Animal{Name: "Toto", UserID: dorothy.ID})
			Expect(err).NotTo(HaveOccurred())
			Expect(animal.ID.IsZero()).To(BeFalse())
		})

		AfterEach(func() {
			client := GetBeamScopedInstance[*mongo.Client]()
			if client != nil {
				Expect(client.Disconnect(context.Background())).NotTo(HaveOccurred())
			}
		})

		It("auto-configures MongoTransactional from Configuration client and database", func() {
			client := GetBeamScopedInstance[*mongo.Client]()
			Expect(client).NotTo(BeNil())
			db := GetBeamScopedInstance[*mongo.Database]()
			Expect(db).NotTo(BeNil())
			Expect(db.Name()).To(Equal("rays_mongo_test"))
			Expect(GetBeamScopedInstance[MongoTransactional]()).NotTo(BeNil())
			Expect(animalRepository.Collection().Name()).To(Equal("custom_animals"))
			Expect(userRepository.Collection().Name()).To(Equal("users"))
		})

		It("can find a record by its ID and check ExistsById and Count", func() {
			opt := userRepository.FindById(dorothy.ID)
			u, ok := opt.Get()
			Expect(ok).To(BeTrue())
			Expect(u.ID).To(Equal(dorothy.ID))
			Expect(u.UserName).To(Equal("dorothy"))

			exists, err := userRepository.ExistsById(dorothy.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			count, err := userRepository.Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(int64(1)))

			all, err := userRepository.FindAll()
			Expect(err).NotTo(HaveOccurred())
			Expect(all).To(HaveLen(1))
		})

		It("can find a record using derived methods", func() {
			u := userRepository.FindByUserName("dorothy")
			Expect(u).NotTo(BeNil())
			Expect(u.ID).To(Equal(dorothy.ID))
			Expect(u.UserName).To(Equal("dorothy"))
		})

		It("can find a record with Error", func() {
			u, err := userRepository.FindWithErrorByUserName("dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(u.ID).To(Equal(dorothy.ID))

			_, err = userRepository.FindWithErrorByUserName("toto")
			Expect(err).To(HaveOccurred())
		})

		It("can find many records and nested document fields", func() {
			users := userRepository.FindAllByName("Dorothy")
			Expect(users).To(HaveLen(1))
			Expect(users[0].ID).To(Equal(dorothy.ID))

			cityUsers := userRepository.FindByAddressCity("Emerald City")
			Expect(cityUsers).To(HaveLen(1))
			Expect(cityUsers[0].UserName).To(Equal("dorothy"))
		})

		It("supports CountBy, ExistsBy, and DeleteBy derived methods", func() {
			count, err := userRepository.CountByName("Dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(int64(1)))

			exists, err := userRepository.ExistsByUserName("dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			deleted, err := userRepository.DeleteByUserName("dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(deleted).To(Equal(int64(1)))

			existsAfter, err := userRepository.ExistsByUserName("dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(existsAfter).To(BeFalse())
		})

		It("can find many records returning a Stream", func() {
			_, err := userRepository.SaveAll([]*User{
				{Name: "Dorothy", UserName: "dorothy2", Age: 30},
				{Name: "Dorothy", UserName: "dorothy3", Age: 40},
			})
			Expect(err).NotTo(HaveOccurred())

			st := userRepository.FindStreamByName("Dorothy")
			names := st.Filter(func(user User) bool { return user.Age >= 30 }).
				SortedBy(func(user User) int { return int(user.Age) }).
				Map(func(user User) string { return user.UserName }).
				ToSlice()
			Expect(names).To(Equal([]string{"dorothy2", "dorothy3"}))

			Expect(userRepository.StreamAllByName("Dorothy").Count()).To(BeNumerically("==", 3))

			stErr, err := userRepository.FindStreamWithErrorByName("Dorothy")
			Expect(err).NotTo(HaveOccurred())
			Expect(stErr.Count()).To(BeNumerically("==", 3))
		})

		It("rejects invalid derived function signatures", func() {
			Expect(userRepository.detectAllByName).To(BeNil())
			Expect(userRepository.findBadName).To(BeNil())
			Expect(userRepository.findBadByNameIsHave).To(BeNil())
			Expect(userRepository.findBadByNameAndAge).To(BeNil())
			Expect(userRepository.findBadByFunction).To(BeNil())
			Expect(userRepository.findBadPointerByName).To(BeNil())
			Expect(userRepository.findBadStructByName).To(BeNil())
			Expect(userRepository.findBadSliceByName).To(BeNil())
			Expect(userRepository.findBadStreamByName).To(BeNil())
			Expect(userRepository.findBadElemByName).To(BeNil())
			Expect(userRepository.findByNameOrderByFoo).To(BeNil())
			Expect(userRepository.findByFoo).To(BeNil())
			Expect(userRepository.findBadByAge).To(BeNil())
			Expect(userRepository.findBadByAgeIn).To(BeNil())
			Expect(userRepository.findBadTypeByName).To(BeNil())
			Expect(userRepository.countBadReturnByName).To(BeNil())
			Expect(userRepository.existsBadReturnByName).To(BeNil())
		})

		It("can find optionally a record", func() {
			opt := userRepository.FindOneByName("Dorothy")
			u, ok := opt.Get()
			Expect(ok).To(BeTrue())
			Expect(u.ID).To(Equal(dorothy.ID))

			_, ok = userRepository.FindOneByName("Alice").Get()
			Expect(ok).To(BeFalse())
		})

		It("supports In, NotIn, Between, GreaterThan, LessThan, Or, Like, String matchers, Null, and Bool operators", func() {
			_, err := userRepository.SaveAll([]*User{
				{Name: "Wendy", UserName: "wendy", Age: 30, Active: false},
				{Name: "Alice", UserName: "alice", Age: 35, Active: true},
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(userRepository.FindAllByAgeIn([]uint{25, 30})).To(HaveLen(2))
			Expect(userRepository.findAllByAgeNotIn([]uint{25, 30})).To(HaveLen(1))
			Expect(userRepository.findAllByAgeGreaterThan(25)).To(HaveLen(2))
			Expect(userRepository.findAllByAgeGreaterThanOrEqual(30)).To(HaveLen(2))
			Expect(userRepository.findAllByAgeLessThan(35)).To(HaveLen(2))
			Expect(userRepository.findAllByAgeLessThanOrEqual(30)).To(HaveLen(2))
			Expect(userRepository.findByNameNot("Dorothy")).To(HaveLen(2))
			Expect(userRepository.findByNameOrName("Dorothy", "Wendy")).To(HaveLen(2))
			Expect(userRepository.findAllByUserNameAndAge("dorothy", 25)).To(HaveLen(1))
			Expect(userRepository.findByEmailIsNotNull()).To(HaveLen(1))
			Expect(userRepository.findByEmailIsNull()).To(HaveLen(2))
			Expect(userRepository.findByNameLike("Dor%")).To(HaveLen(1))
			Expect(userRepository.findByNameNotLike("Dor%")).To(HaveLen(2))
			Expect(userRepository.findByNameStartingWith("Ali")).To(HaveLen(1))
			Expect(userRepository.findByNameEndingWith("thy")).To(HaveLen(1))
			Expect(userRepository.findByNameContaining("end")).To(HaveLen(1))
			Expect(userRepository.findByActiveTrue()).To(HaveLen(2))
			Expect(userRepository.findByActiveFalse()).To(HaveLen(1))
		})

		It("supports Between and single/multi-field OrderBy", func() {
			_, err := userRepository.SaveAll([]*User{
				{Name: "Wendy", UserName: "wendy", Age: 30},
				{Name: "Alice", UserName: "alice", Age: 35},
			})
			Expect(err).NotTo(HaveOccurred())

			asc := userRepository.FindAllByAgeBetweenOrderByAgeAsc(20, 30)
			Expect(asc).To(HaveLen(2))
			Expect(asc[0].Age).To(Equal(uint(25)))

			desc := userRepository.FindAllByAgeBetweenOrderByAgeDesc(20, 30)
			Expect(desc).To(HaveLen(2))
			Expect(desc[0].Age).To(Equal(uint(30)))

			multi := userRepository.findAllByAgeBetweenOrderByNameAscAgeAsc(20, 40)
			Expect(multi).To(HaveLen(3))
			Expect(multi[0].Name).To(Equal("Alice"))
			Expect(multi[1].Name).To(Equal("Dorothy"))
			Expect(multi[2].Name).To(Equal("Wendy"))
		})

		It("supports variadic MongoQueryFunc, *MongoQuery return, Distinct, and Aggregate", func() {
			_, err := userRepository.SaveAll([]*User{
				{Name: "Wendy", UserName: "wendy", Age: 30},
				{Name: "Alice", UserName: "alice", Age: 35},
			})
			Expect(err).NotTo(HaveOccurred())

			users := userRepository.findAllByAgeBetween(20, 30, OrderByDesc("name"))
			Expect(users).To(HaveLen(2))
			Expect(users[0].Name).To(Equal("Wendy"))
			Expect(users[1].Name).To(Equal("Dorothy"))

			var qUsers []User
			Expect(userRepository.queryAllByAgeBetween(20, 30).Find(&qUsers)).NotTo(HaveOccurred())
			Expect(qUsers).To(HaveLen(2))

			var ages []uint
			Expect(userRepository.queryAllByAgeBetween(20, 30).OrderByAsc("age").Distinct("age", &ages)).NotTo(HaveOccurred())
			Expect(ages).To(ConsistOf(uint(25), uint(30)))

			// Derived *MongoQuery and MongoRepository.Query() execute lazily and propagate terminal errors to the captured transaction
			txMgr := GetBeamScopedInstance[TransactionManager]()
			txErr := txMgr.Transactional(func() Error {
				_, saveErr := userRepository.Save(&User{Name: "LazyRollback", UserName: "lazy_rollback", Age: 28})
				Expect(saveErr).NotTo(HaveOccurred())
				q := userRepository.queryAllByAgeBetween(20, 30)
				Expect(q.Error()).NotTo(HaveOccurred())
				Expect(q.Find(nil)).To(HaveOccurred())
				return nil
			})
			Expect(txErr).To(HaveOccurred())
			Expect(userRepository.FindByName("LazyRollback")).To(BeNil())

			txErr = txMgr.Transactional(func() Error {
				_, saveErr := userRepository.Save(&User{Name: "RepoQueryRollback", UserName: "repo_query_rollback", Age: 28})
				Expect(saveErr).NotTo(HaveOccurred())
				q := userRepository.Query()
				Expect(q.Find(nil)).To(HaveOccurred())
				return nil
			})
			Expect(txErr).To(HaveOccurred())
			Expect(userRepository.FindByName("RepoQueryRollback")).To(BeNil())

			// A retained query built outside a transaction cannot mark or roll back a later transaction
			retainedAmbientQuery := userRepository.queryAllByAgeBetween(20, 30)
			Expect(txMgr.Transactional(func() Error {
				Expect(retainedAmbientQuery.Find(nil)).To(HaveOccurred())
				_, saveErr := userRepository.Save(&User{Name: "SurvivesRetainedQuery", UserName: "survives_retained", Age: 29})
				return saveErr
			})).NotTo(HaveOccurred())
			survived := userRepository.FindByName("SurvivesRetainedQuery")
			Expect(survived).NotTo(BeNil())
			Expect(userRepository.DeleteById(survived.ID)).NotTo(HaveOccurred())

			// Rebinding a query via WithContext to an active MongoSession (directly or wrapped) reports errors to that session,
			// while rebinding to a non-session context detaches error reporting from the transaction.
			mongoTx := GetBeamScopedInstance[MongoTransactional]()
			Expect(txMgr.Transactional(func() Error {
				_, saveErr := userRepository.Save(&User{Name: "ReboundDirectRollback", UserName: "rebound_direct", Age: 29})
				Expect(saveErr).NotTo(HaveOccurred())
				Expect(retainedAmbientQuery.WithContext(mongoTx.Get()).Find(nil)).To(HaveOccurred())
				return nil
			})).To(HaveOccurred())
			Expect(userRepository.FindByName("ReboundDirectRollback")).To(BeNil())

			Expect(txMgr.Transactional(func() Error {
				_, saveErr := userRepository.Save(&User{Name: "ReboundWrappedRollback", UserName: "rebound_wrapped", Age: 29})
				Expect(saveErr).NotTo(HaveOccurred())
				wrappedSessCtx, cancel := context.WithTimeout(mongoTx.Get(), 5*time.Second)
				defer cancel()
				Expect(retainedAmbientQuery.WithContext(wrappedSessCtx).Find(nil)).To(HaveOccurred())
				return nil
			})).To(HaveOccurred())
			Expect(userRepository.FindByName("ReboundWrappedRollback")).To(BeNil())

			Expect(txMgr.Transactional(func() Error {
				inTxQuery := userRepository.Query()
				Expect(inTxQuery.WithContext(context.Background()).Find(nil)).To(HaveOccurred())
				_, saveErr := userRepository.Save(&User{Name: "DetachedQuerySurvives", UserName: "detached_survives", Age: 29})
				return saveErr
			})).NotTo(HaveOccurred())
			detachedSurvived := userRepository.FindByName("DetachedQuerySurvives")
			Expect(detachedSurvived).NotTo(BeNil())
			Expect(userRepository.DeleteById(detachedSurvived.ID)).NotTo(HaveOccurred())

			type AgeStats struct {
				Total int `bson:"total"`
			}
			var stats []AgeStats
			err = userRepository.Query().Aggregate(mongo.Pipeline{
				{{Key: "$group", Value: bson.D{
					{Key: "_id", Value: nil},
					{Key: "total", Value: bson.D{{Key: "$sum", Value: "$age"}}},
				}}},
			}, &stats)
			Expect(err).NotTo(HaveOccurred())
			Expect(stats).To(HaveLen(1))
			Expect(stats[0].Total).To(Equal(90))
		})

		It("can update and delete records", func() {
			user, ok := userRepository.FindById(dorothy.ID).Get()
			Expect(ok).To(BeTrue())
			user.Name = "foo"
			_, err := userRepository.Save(user)
			Expect(err).NotTo(HaveOccurred())

			updated, ok := userRepository.FindById(dorothy.ID).Get()
			Expect(ok).To(BeTrue())
			Expect(updated.Name).To(Equal("foo"))

			Expect(userRepository.Delete(updated)).NotTo(HaveOccurred())
			Expect(userRepository.FindById(dorothy.ID).IsPresent()).To(BeFalse())

			another, err := userRepository.Save(&User{Name: "Temp", UserName: "temp", Age: 18})
			Expect(err).NotTo(HaveOccurred())
			Expect(userRepository.DeleteById(another.ID)).NotTo(HaveOccurred())
			Expect(userRepository.FindById(another.ID).IsPresent()).To(BeFalse())
		})

		It("supports ThreadLocal transactions with commit, rollback, and manual error after rollback", func() {
			userService.TransactionalChangeRollback(dorothy.ID)
			userService.TransactionalChangeRollbackPprof(dorothy.ID)
			userService.TransactionalRepositorySetErrorRollback(dorothy.ID)
			userService.TransactionalChangeCommit(dorothy.ID)
			userService.ErrorAfterRollback(dorothy.ID)
		})
	})

	Context("Type Splitting & Context Splitting", func() {
		It("splits MongoDB connections by Entity Type T and coordinates multi-connection commit and rollback", func() {
			GetContainer().Reset()

			userClient, err := NewMongoClientBuilder().WithHost(mongoHost).WithPort(mongoPort).WithDirectConnection(true).Build()
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = userClient.Disconnect(context.Background()) })

			animalClient, err := NewMongoClientBuilder().WithHost(mongoHost).WithPort(mongoPort).WithDirectConnection(true).Build()
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = animalClient.Disconnect(context.Background()) })

			userDB := userClient.Database("type_split_users_db")
			animalDB := animalClient.Database("type_split_animals_db")
			Expect(userDB.Drop(context.Background())).NotTo(HaveOccurred())
			Expect(animalDB.Drop(context.Background())).NotTo(HaveOccurred())

			userRepo := &UserRepository{}
			animalRepo := &AnimalRepository{}
			AddBeamFor(&typeSplitMongoDatabases{userDB: userDB, animalDB: animalDB})
			AddBeamFor(&typeSplitMongoConfig{})
			AddBeamFor(userRepo)
			AddBeamFor(animalRepo)
			GetContainer().EnableScanning()
			Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

			Expect(userRepo.Collection().Database().Name()).To(Equal("type_split_users_db"))
			Expect(animalRepo.Collection().Database().Name()).To(Equal("type_split_animals_db"))

			txMgr := GetBeamScopedInstance[TransactionManager]()

			// 1. Coordinated multi-connection Commit across userDB (userClient) and animalDB (animalClient)
			var savedUser *User
			var savedAnimal *Animal
			Expect(txMgr.Transactional(func() Error {
				var saveErr Error
				savedUser, saveErr = userRepo.Save(&User{Name: "Dorothy", UserName: "dorothy", Age: 25})
				Expect(saveErr).NotTo(HaveOccurred())
				savedAnimal, saveErr = animalRepo.Save(&Animal{Name: "Toto", UserID: savedUser.ID})
				Expect(saveErr).NotTo(HaveOccurred())
				return nil
			})).NotTo(HaveOccurred())

			Expect(userRepo.FindById(savedUser.ID).IsPresent()).To(BeTrue())
			Expect(animalRepo.FindById(savedAnimal.ID).IsPresent()).To(BeTrue())

			// 2. Coordinated multi-connection Rollback: error after saving in both rolls back BOTH connections
			var rolledBackAnimal *Animal
			Expect(txMgr.Transactional(func() Error {
				u, saveErr := userRepo.Save(&User{Name: "Glinda", UserName: "glinda", Age: 30})
				Expect(saveErr).NotTo(HaveOccurred())
				rolledBackAnimal, saveErr = animalRepo.Save(&Animal{Name: "Lion", UserID: u.ID})
				Expect(saveErr).NotTo(HaveOccurred())
				return NewError("AbortBoth", "rollback both MongoDB connections")
			})).To(HaveOccurred())

			Expect(userRepo.FindByName("Glinda")).To(BeNil())
			Expect(animalRepo.FindById(rolledBackAnimal.ID).IsPresent()).To(BeFalse())
		})

		It("splits MongoDB connections by Repository Type", func() {
			GetContainer().Reset()

			client, err := NewMongoClientBuilder().WithHost(mongoHost).WithPort(mongoPort).WithDirectConnection(true).Build()
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = client.Disconnect(context.Background()) })

			userDB := client.Database("repo_split_users_db")
			animalDB := client.Database("repo_split_animals_db")
			Expect(userDB.Drop(context.Background())).NotTo(HaveOccurred())
			Expect(animalDB.Drop(context.Background())).NotTo(HaveOccurred())

			userRepo := &UserRepository{}
			animalRepo := &AnimalRepository{}
			AddBeamFor(&typeSplitMongoDatabases{userDB: userDB, animalDB: animalDB})
			AddBeamFor(&repoTypeSplitMongoConfig{})
			AddBeamFor(userRepo)
			AddBeamFor(animalRepo)
			GetContainer().EnableScanning()
			Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

			Expect(userRepo.Collection().Database().Name()).To(Equal("repo_split_users_db"))
			Expect(animalRepo.Collection().Database().Name()).To(Equal("repo_split_animals_db"))

			u, err := userRepo.Save(&User{Name: "Oz", UserName: "oz", Age: 50})
			Expect(err).NotTo(HaveOccurred())
			a, err := animalRepo.Save(&Animal{Name: "Tiger", UserID: u.ID})
			Expect(err).NotTo(HaveOccurred())

			Expect(userRepo.FindById(u.ID).IsPresent()).To(BeTrue())
			Expect(animalRepo.FindById(a.ID).IsPresent()).To(BeTrue())

			// Also verify helper functions on empty context and direct ParentType/ParentInstance context
			Expect(MongoRepositoryTypeFromContext(context.Background())).To(BeNil())
			Expect(MongoEntityTypeFromContext(context.Background())).To(BeNil())
			parentCtx := context.WithValue(context.Background(), PrototypeContextParentType, reflect.TypeFor[*UserRepository]())
			parentCtx = context.WithValue(parentCtx, PrototypeContextParentInstance, userRepo)
			Expect(MongoRepositoryTypeFromContext(parentCtx)).To(Equal(reflect.TypeFor[*UserRepository]()))
			Expect(MongoEntityTypeFromContext(parentCtx)).To(Equal(reflect.TypeFor[User]()))
		})

		It("splits MongoDB connections dynamically by context.Context via MongoTransactionalResolver and Foundry", func() {
			GetContainer().Reset()

			clientA, err := NewMongoClientBuilder().WithHost(mongoHost).WithPort(mongoPort).WithDirectConnection(true).Build()
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = clientA.Disconnect(context.Background()) })

			clientB, err := NewMongoClientBuilder().WithHost(mongoHost).WithPort(mongoPort).WithDirectConnection(true).Build()
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = clientB.Disconnect(context.Background()) })

			defaultDB := clientA.Database("ctx_split_default_db")
			tenantADB := clientA.Database("ctx_split_tenant_a_db")
			tenantBDB := clientB.Database("ctx_split_tenant_b_db")
			Expect(defaultDB.Drop(context.Background())).NotTo(HaveOccurred())
			Expect(tenantADB.Drop(context.Background())).NotTo(HaveOccurred())
			Expect(tenantBDB.Drop(context.Background())).NotTo(HaveOccurred())

			repo := &contextSplitUserRepository{}
			AddBeamFor(&contextSplitMongoDatabases{
				defaultDB: defaultDB,
				tenantADB: tenantADB,
				tenantBDB: tenantBDB,
			})
			AddBeamFor(&contextSplitMongoConfig{})
			AddBeamFor(repo)
			GetContainer().EnableScanning()
			Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

			txMgr := GetBeamScopedInstance[TransactionManager]()

			// Tenant A transaction
			ctxA := context.WithValue(context.Background(), tenantKey, "tenantA")
			txMgr.SetContext(ctxA)
			Expect(txMgr.Transactional(func() Error {
				_, saveErr := repo.Save(&User{Name: "AliceA", UserName: "alice_a", Age: 20})
				Expect(saveErr).NotTo(HaveOccurred())
				// Second call in same context hits goroutine txCache
				Expect(repo.FindByName("AliceA")).NotTo(BeNil())
				return nil
			})).NotTo(HaveOccurred())
			txMgr.ClearContext()

			// Tenant B transaction
			ctxB := context.WithValue(context.Background(), tenantKey, "tenantB")
			txMgr.SetContext(ctxB)
			Expect(txMgr.Transactional(func() Error {
				// AliceA does not exist in Tenant B's database
				Expect(repo.FindByName("AliceA")).To(BeNil())
				_, saveErr := repo.Save(&User{Name: "BobB", UserName: "bob_b", Age: 30})
				return saveErr
			})).NotTo(HaveOccurred())
			txMgr.ClearContext()

			// Verify isolation between Tenant A, Tenant B, and Default DB
			txMgr.SetContext(ctxA)
			Expect(repo.FindByName("AliceA")).NotTo(BeNil())
			Expect(repo.FindByName("BobB")).To(BeNil())
			txMgr.ClearContext()

			txMgr.SetContext(ctxB)
			Expect(repo.FindByName("BobB")).NotTo(BeNil())
			Expect(repo.FindByName("AliceA")).To(BeNil())
			txMgr.ClearContext()

			// Fallback to default transactionManager when resolver returns nil (no tenantKey in context)
			Expect(repo.FindByName("AliceA")).To(BeNil())
			Expect(repo.FindByName("BobB")).To(BeNil())

			// Context with incomparable dynamic value (TypeOf.Comparable() == true, ValueOf.Comparable() == false) avoids panic
			incompCtx := dynamicPayloadCtx{
				Context: ctxA,
				payload: []string{"incomparable-slice-in-interface-field"},
			}
			txMgr.SetContext(incompCtx)
			Expect(repo.FindByName("AliceA")).NotTo(BeNil())
			Expect(repo.FindByName("AliceA")).NotTo(BeNil())
			txMgr.ClearContext()

			// Unknown tenant propagates routing failure and stops the transaction
			ctxUnknown := context.WithValue(context.Background(), tenantKey, "unknownTenant")
			txMgr.SetContext(ctxUnknown)
			unknownErr := txMgr.Transactional(func() Error {
				_, saveErr := repo.Save(&User{Name: "Blocked", UserName: "blocked", Age: 40})
				return saveErr
			})
			Expect(unknownErr).To(HaveOccurred())
			Expect(unknownErr.Name()).To(Equal("UnknownTenantError"))
			txMgr.ClearContext()
		})
	})
})

type tenantCtxKey string

const tenantKey tenantCtxKey = "tenant"

type dynamicPayloadCtx struct {
	context.Context
	payload any
}

type typeSplitMongoDatabases struct {
	userDB   *mongo.Database
	animalDB *mongo.Database
}

type typeSplitMongoConfig struct {
	Configuration
	dbs *typeSplitMongoDatabases `@:"Inject"`
}

func (this *typeSplitMongoConfig) Database(ctx context.Context) *mongo.Database {
	if MongoEntityTypeFromContext(ctx) == reflect.TypeFor[Animal]() {
		return this.dbs.animalDB
	}
	return this.dbs.userDB
}

type repoTypeSplitMongoConfig struct {
	Configuration
	dbs *typeSplitMongoDatabases `@:"Inject"`
}

func (this *repoTypeSplitMongoConfig) Database(ctx context.Context) *mongo.Database {
	if MongoRepositoryTypeFromContext(ctx) == reflect.TypeFor[*AnimalRepository]() {
		return this.dbs.animalDB
	}
	return this.dbs.userDB
}

type contextSplitUserRepository struct {
	foundry    Foundry[MongoTransactional] `@:"Inject"`
	findByName func(string) *User
	MongoRepository[User, primitive.ObjectID]
}

var _ MongoTransactionalResolver = &contextSplitUserRepository{}

func (this *contextSplitUserRepository) MongoTransactional(ctx context.Context) MongoTransactional {
	if ctx.Value(tenantKey) == nil {
		return nil
	}
	tx := RaiseVal(this.foundry.Forge(ctx))
	if tx.Database() == nil {
		RaiseErr(NewError("UnknownTenantError", "unrecognized tenant in context"))
	}
	return tx
}

func (this *contextSplitUserRepository) FindByName(name string) *User {
	return this.findByName(name)
}

type contextSplitMongoDatabases struct {
	defaultDB *mongo.Database
	tenantADB *mongo.Database
	tenantBDB *mongo.Database
}

type contextSplitMongoConfig struct {
	Configuration
	dbs *contextSplitMongoDatabases `@:"Inject"`
}

func (this *contextSplitMongoConfig) Database(ctx context.Context) (*mongo.Database, Error) {
	switch ctx.Value(tenantKey) {
	case nil:
		return this.dbs.defaultDB, nil
	case "tenantA":
		return this.dbs.tenantADB, nil
	case "tenantB":
		return this.dbs.tenantBDB, nil
	default:
		return nil, NewError("UnknownTenantError", "unrecognized tenant in context")
	}
}
