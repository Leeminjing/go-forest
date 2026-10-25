package entry

import (
	"context"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

func HTTP(f *forest.Forest, address string) (forest.Definition, error) {
	return ginforest.Definition(f, ginforest.Options{TypeID: "frameworks.http", Version: "1", Address: address, Ports: ports(), BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
		ref, err := forest.Reference[worker](env, "business")
		if err != nil {
			return nil, err
		}
		r := gin.New()
		r.Use(ginforest.Scope(f))
		r.GET("/work", func(c *gin.Context) {
			value, err := read(c.Request.Context(), f, ref, c.Query("hold") == "1")
			if err != nil {
				c.JSON(503, gin.H{"error": "unavailable"})
				return
			}
			c.JSON(200, value)
		})
		return r, nil
	}})
}
