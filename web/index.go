package boltbrowserweb

import (
	"bytes"
	"encoding/hex"
	"fmt"

	bbntypes "github.com/babylonlabs-io/babylon/types"
	bbnproto "github.com/babylonlabs-io/finality-provider/finality-provider/proto"
	bbnprotoold "github.com/evnix/boltdbweb/altproto"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	pm "google.golang.org/protobuf/proto"
)

var Db *bbolt.DB

func Index(c *gin.Context) {

	c.Redirect(301, "/web/html/layout.html")

}

func CreateBucket(c *gin.Context) {

	if c.PostForm("bucket") == "" {
		c.String(200, "no bucket name | n")
	}

	Db.Update(func(tx *bbolt.Tx) error {
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

	Db.Update(func(tx *bbolt.Tx) error {
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

	Db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
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

func tryParseHex(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

func Put(c *gin.Context) {

	if c.PostForm("bucket") == "" || c.PostForm("key") == "" {
		c.String(200, "no bucket name or key | n")
	}

	Db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(c.PostForm("bucket")))
		if err != nil {

			c.String(200, "error  creating bucket | n")
			return fmt.Errorf("create bucket: %s", err)
		}

		key, err := tryParseHex(c.PostForm("key"))
		if err != nil {
			key = []byte(c.PostForm("key"))
		}

		value, err := tryParseHex(c.PostForm("value"))
		if err != nil {
			value = []byte(c.PostForm("value"))
		}

		// TODO: this is a hack
		if c.PostForm("key") == "eacf2435ba6c2a29a58aba776e3cccbe7c54def6402518e63e9b3b042dbefcc4" && c.PostForm("value") == "" {
			log.Info("Modifying key eacf2435ba6c2a29a58aba776e3cccbe7c54def6402518e63e9b3b042dbefcc4. This is a hack")
			// get the value from the db
			oldValue := b.Get(key)

			// unmarshal the value to a FinalityProvider (old)
			fpOld := &bbnprotoold.FinalityProvider{}
			if err := pm.Unmarshal(oldValue, fpOld); err != nil {
				c.String(200, "error unmarshalling value | n")
				return fmt.Errorf("unmarshalling value: %s", err)
			}

			// use the old FinalityProvider to create a new FinalityProvider
			fpNew := &bbnproto.FinalityProvider{
				FpAddr:      "bbn1p4eyheg4quhxjrx7r3029hxylugl546zyfs7l9",
				BtcPk:       fpOld.BtcPk,
				Description: fpOld.Description,
				Commission:  fpOld.Commission,
				Pop: &bbnproto.ProofOfPossession{
					BtcSig: fpOld.Pop.BtcSig,
				},
				KeyName:             fpOld.KeyName,
				ChainId:             fpOld.ChainId,
				LastVotedHeight:     fpOld.LastVotedHeight,
				LastProcessedHeight: fpOld.LastProcessedHeight,
				Status:              bbnproto.FinalityProviderStatus(fpOld.Status),
			}

			value, err = pm.Marshal(fpNew)
			if err != nil {
				c.String(200, "error marshalling value | n")
				return fmt.Errorf("marshalling value: %s", err)
			}
		}

		err = b.Put(key, value)

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

	Db.View(func(tx *bbolt.Tx) error {

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
	// if true {
	// 	return fmt.Sprintf("%x", v)
	// }
	// try to parse as a BIP340 pubkey
	pb, err := bbntypes.NewBIP340PubKey(v)
	if err == nil {
		return pb.MarshalHex()
	}

	return string(v)
}

func tryParseBytesArrayValue(v []byte) string {
	// if true {
	// 	return fmt.Sprintf("%x", v)
	// }

	fpNew := &bbnproto.FinalityProvider{}
	if err := pm.Unmarshal(v, fpNew); err == nil {
		return fpNew.String()
	}

	fpOld := &bbnprotoold.FinalityProvider{}
	if err := pm.Unmarshal(v, fpOld); err == nil {
		return fpOld.String()
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

		Db.View(func(tx *bbolt.Tx) error {
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

		Db.View(func(tx *bbolt.Tx) error {
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

	Db.View(func(tx *bbolt.Tx) error {

		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {

			b := []string{string(name)}
			res = append(res, b...)
			return nil
		})

	})

	c.JSON(200, res)

}
