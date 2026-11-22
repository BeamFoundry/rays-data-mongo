// # Initialization
//
// This package should be imported from a main file, alongside rays:
//
//	package main
//
//	import (
//	  "github.com/BeamFoundry/rays"
//	  _ "github.com/BeamFoundry/rays-data-mongo"
//	)
//
// # Packages
//
// This project divides its code between the following packages:
//
//   - [github.com/BeamFoundry/rays-data-mongo/pkg/core]: Contains repository interfaces, query builder, and fluent MongoQuery types.
//   - [github.com/BeamFoundry/rays-data-mongo/pkg/tx]: Contains MongoClientBuilder, MongoSession, MongoTransactional, and MongoTransactionalManager.
//   - [github.com/BeamFoundry/rays-data-mongo/pkg/util]: Contains MongoLogSink, query helpers, and MongoQueryFunc utilities.
package rays_data_mongo //nolint:revive // Ignoring package naming hint

import (
	_ "github.com/BeamFoundry/rays"                                // rays
	_ "github.com/BeamFoundry/rays-data-mongo/internal/autoconfig" // autoconfig
	_ "github.com/BeamFoundry/rays-data-mongo/pkg/core"            // core
	_ "github.com/BeamFoundry/rays-data-mongo/pkg/tx"              // tx
)
