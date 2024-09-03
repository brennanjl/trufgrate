package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kwilteam/kwil-db/core/client"
	"github.com/kwilteam/kwil-db/core/crypto"
	"github.com/kwilteam/kwil-db/core/crypto/auth"
	"github.com/kwilteam/kwil-db/core/types"
	ctypes "github.com/kwilteam/kwil-db/core/types/client"
	"github.com/kwilteam/kwil-db/parse"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "trufgrate",
		Short: "Trufgrate is a tool to migrate Truflation Kuneiform contracts.",
	}

	rootCmd.AddCommand(primitiveCmd())
	rootCmd.AddCommand(composedCmd())

	return rootCmd
}

// setupMigration is a utility function to set up migrations for both primitive and composed contracts
func setupMigration(ctx context.Context, rpc, privateKey, schemaPath string) (ctypes.Client, auth.Signer, *types.Schema, error) {
	signer, err := makeSigner(privateKey)
	if err != nil {
		return nil, nil, nil, err
	}

	cl, err := client.NewClient(ctx, rpc, &ctypes.Options{
		Signer: signer,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	schema, err := readInSchema(schemaPath)
	if err != nil {
		return nil, nil, nil, err
	}

	return cl, signer, schema, nil
}

func readInSchema(path string) (*types.Schema, error) {
	// read file
	schemaBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// parse schema
	schema, err := parse.Parse(schemaBytes)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func makeSigner(keyHex string) (auth.Signer, error) {
	key, err := crypto.Secp256k1PrivateKeyFromHex(keyHex)
	if err != nil {
		return nil, err
	}
	return &auth.EthPersonalSigner{Key: *key}, nil
}

// contains checks that a contains b.
// it does not take into account the order of the elements.
// If one is missing, it returns false and the missing element.
func contains(a []string, b []string) (bool, string) {
	aMap := make(map[string]struct{}, len(a))
	for _, v := range a {
		aMap[v] = struct{}{}
	}

	for _, v := range b {
		if _, ok := aMap[v]; !ok {
			return false, v
		}
	}

	return true, ""
}
