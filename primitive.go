package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/types/client"
	ctypes "github.com/kwilteam/kwil-db/core/types/client"
	"github.com/spf13/cobra"
)

func primitiveCmd() *cobra.Command {
	var schemas []string
	var privateKey string
	var rpc string
	var primitiveFile string
	var schema string

	primitiveCmd := &cobra.Command{
		Use:   "primitive",
		Short: "Migrate primitive contracts",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, signer, schema, err := setupMigration(ctx, privateKey, rpc, schema)
			if err != nil {
				return err
			}

			// Read the primitive CSV file
			data, err := readPrimitiveCSV(primitiveFile)
			if err != nil {
				return err
			}

			// if user specified specific schemas, check that they exist in the CSV file
			var allSchemas []string
			for _, d := range data {
				allSchemas = append(allSchemas, d.StreamID)
			}

			if len(schemas) > 0 {
				if ok, missing := contains(allSchemas, schemas); !ok {
					return fmt.Errorf("schema %s not found in primitive CSV file", missing)
				}

				allSchemas = schemas
			}

			// now we want to list all deployed schemas and ensure that all schemas we plan to migrate are deployed
			deployedSchemas, err := client.ListDatabases(ctx, signer.Identity())
			if err != nil {
				return err
			}

			var deployedSchemaList []string
			for _, s := range deployedSchemas {
				deployedSchemaList = append(deployedSchemaList, s.Name)
			}

			if ok, missing := contains(deployedSchemaList, allSchemas); !ok {
				return fmt.Errorf("schema %s not deployed", missing)
			}

			// migrate each schema
			for _, s := range allSchemas {
				if err := migratePrimitive(ctx, client, schema, s); err != nil {
					return err
				}
			}

			return nil
		},
	}

	primitiveCmd.Flags().StringSliceVarP(&schemas, "schemas", "s", []string{}, "Schemas to migrate. If empty, all schemas will be migrated.")
	primitiveCmd.Flags().StringVarP(&privateKey, "private-key", "k", "", "Private key to use for migration")
	primitiveCmd.Flags().StringVarP(&rpc, "rpc", "r", "", "RPC to use for migration")
	primitiveCmd.Flags().StringVarP(&primitiveFile, "primitive-file", "f", "", "Primitive CSV file to use for migration")
	primitiveCmd.Flags().StringVarP(&schema, "schema", "c", "", "Schema file to use for migration")

	// mark the flags as required
	primitiveCmd.MarkFlagRequired("private-key")
	primitiveCmd.MarkFlagRequired("rpc")
	primitiveCmd.MarkFlagRequired("primitive-file")
	primitiveCmd.MarkFlagRequired("schema")

	return primitiveCmd
}

// migratePrimitive migrates a primitive contract.
func migratePrimitive(ctx context.Context, client client.Client, schema *types.Schema, streamdID string) error {
	// drop the stream
	_, err := client.DropDatabase(ctx, streamdID, ctypes.WithSyncBroadcast(true))
	if err != nil {
		return err
	}

	// deploy the stream
	schema.Name = streamdID
	_, err = client.DeployDatabase(ctx, schema, ctypes.WithSyncBroadcast(true))
	if err != nil {
		return err
	}

	// TODO: @TSN team should add logic for uploading the primitive data here
	return nil
}

// mocks https://github.com/truflation/tsn-data-provider/blob/f0837831b36cbca4201252684ac9a19ae4ae0b1d/assets/categories/cpi_uk/primitive_sources.csv
type PrimitiveStreamData struct {
	StreamID        string
	SourceType      string
	SourceID        string
	UpdateFrequency int
}

// reads in https://github.com/truflation/tsn-data-provider/blob/f0837831b36cbca4201252684ac9a19ae4ae0b1d/assets/categories/cpi_uk/primitive_sources.csv
func readPrimitiveCSV(filePath string) ([]PrimitiveStreamData, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read the first line as the header (optional)
	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("error reading headers: %w", err)
	}

	// Read the rest of the lines
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading records: %w", err)
	}

	var data []PrimitiveStreamData

	// Process each record
	for _, record := range records {
		// Parse update_frequency as an integer
		updateFreq, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, fmt.Errorf("error parsing update_frequency: %w", err)
		}

		// Create a new PrimitiveStreamData struct
		streamData := PrimitiveStreamData{
			StreamID:        record[0],
			SourceType:      record[1],
			SourceID:        record[2],
			UpdateFrequency: updateFreq,
		}

		// Append to the slice
		data = append(data, streamData)
	}

	return data, nil
}
