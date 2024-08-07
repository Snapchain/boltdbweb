package boltbrowserweb

import (
	"bytes"
	"fmt"

	bbntypes "github.com/babylonlabs-io/babylon/types"
	bbnproto "github.com/babylonlabs-io/finality-provider/finality-provider/proto"
	"github.com/boltdb/bolt"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	pm "google.golang.org/protobuf/proto"
)

var Db *bolt.DB

func Index(c *gin.Context) {

	c.Redirect(301, "/web/html/layout.html")

}

func CreateBucket(c *gin.Context) {

	if c.PostForm("bucket") == "" {
		c.String(200, "no bucket name | n")
	}

	Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		b = b
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	c.String(200, "ok")

}

func DeleteBucket(c *gin.Context) {

	if c.PostForm("bucket") == "" {
		c.String(200, "no bucket name | n")
	}

	Db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(c.PostForm("bucket")))

		if err != nil {

			c.String(200, "error no such bucket | n")
			return fmt.Errorf("bucket: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func DeleteKey(c *gin.Context) {

	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {
		c.String(200, "no bucket name or key | n")
	}

	Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		b = b
		if err != nil {

			c.String(200, "error no such bucket | n")
			return fmt.Errorf("bucket: %s", err)
		}

		err = b.Delete([]byte(c.PostForm("key")))

		if err != nil {

			c.String(200, "error Deleting KV | n")
			return fmt.Errorf("delete kv: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func Put(c *gin.Context) {

	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {
		c.String(200, "no bucket name or key | n")
	}

	Db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		b = b
		if err != nil {

			c.String(200, "error  creating bucket | n")
			return fmt.Errorf("create bucket: %s", err)
		}

		err = b.Put([]byte(c.PostForm("key")), []byte(c.PostForm("value")))

		if err != nil {

			c.String(200, "error writing KV | n")
			return fmt.Errorf("create kv: %s", err)
		}

		return nil
	})

	c.String(200, "ok")

}

func Get(c *gin.Context) {

	res := []string{"nok", ""}

	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {

		res[1] = "no bucket name or key | n"
		c.JSON(200, res)
	}

	Db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(c.PostForm("bucket")))

		if b != nil {

			v := tryParseBytesArrayKey(b.Get([]byte(c.PostForm("key"))))

			res[0] = "ok"
			res[1] = v

			log.Infof("Key: %s", v)

		} else {

			res[1] = "error opening bucket| does it exist? | n"

		}
		return nil

	})

	c.JSON(200, res)

}

type Result struct {
	Result string
	M      map[string]string
}

func tryParseBytesArrayKey(v []byte) string {
	// try to parse as a BIP340 pubkey
	pb, err := bbntypes.NewBIP340PubKey(v)
	if err == nil {
		return pb.MarshalHex()
	}

	return string(v)
}

func tryParseBytesArrayValue(v []byte) string {
	// try to parse as a Babylon FinalityProvider
	fp := &bbnproto.FinalityProvider{}
	if err := pm.Unmarshal(v, fp); err == nil {
		return fp.String()
	}

	return string(v)
}

func PrefixScan(c *gin.Context) {

	res := Result{Result: "nok"}

	res.M = make(map[string]string)

	if c.PostForm("bucket") == "" {

		res.Result = "no bucket name | n"
		c.JSON(200, res)
	}

	count := 0

	if c.PostForm("key") == "" {

		Db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(c.PostForm("bucket")))

			if b != nil {

				c := b.Cursor()

				for k, v := c.First(); k != nil; k, v = c.Next() {
					res.M[tryParseBytesArrayKey(k)] = tryParseBytesArrayValue(v)

					if count > 2000 {
						break
					}
					count++
				}

				res.Result = "ok"

			} else {

				res.Result = "no such bucket available | n"

			}

			return nil
		})

	} else {

		Db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(c.PostForm("bucket"))).Cursor()

			if b != nil {

				prefix := []byte(c.PostForm("key"))

				for k, v := b.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = b.Next() {
					res.M[tryParseBytesArrayKey(k)] = tryParseBytesArrayValue(v)
					if count > 2000 {
						break
					}
					count++
				}

				res.Result = "ok"

			} else {

				res.Result = "no such bucket available | n"

			}

			return nil
		})

	}

	c.JSON(200, res)

}

func Buckets(c *gin.Context) {

	res := []string{}

	Db.View(func(tx *bolt.Tx) error {

		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {

			b := []string{string(name)}
			res = append(res, b...)
			return nil
		})

	})

	c.JSON(200, res)

}
