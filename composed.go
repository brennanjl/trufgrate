package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/kwilteam/kwil-db/core/types"
	ctypes "github.com/kwilteam/kwil-db/core/types/client"
	"github.com/kwilteam/kwil-db/core/types/decimal"
	"github.com/spf13/cobra"
)

func composedCmd() *cobra.Command {
	var schemas []string
	var privateKey string
	var rpc string
	var taxonomyFile string
	var schema string

	composedCmd := &cobra.Command{
		Use:   "composed",
		Short: "Migrate composed contracts",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, signer, schema, err := setupMigration(ctx, privateKey, rpc, schema)
			if err != nil {
				return err
			}

			data, err := readTaxonomyCSV(taxonomyFile)
			if err != nil {
				return err
			}

			var allSchemas []string
			for _, d := range data {
				allSchemas = append(allSchemas, d.StreamID)
			}

			if len(schemas) > 0 {
				if ok, missing := contains(allSchemas, schemas); !ok {
					return fmt.Errorf("schema %s not found in taxonomy CSV file", missing)
				}

				allSchemas = schemas
			}

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

			toMigration := removeUnnecessaryTaxonomyData(normalizeTaxonomyData(data), allSchemas)
			for _, d := range toMigration {
				if err := migrateComposed(ctx, client, schema, d.StreamID, d.Children); err != nil {
					return err
				}
			}

			return nil
		},
	}

	composedCmd.Flags().StringSliceVarP(&schemas, "schemas", "s", []string{}, "Schemas to migrate. If empty, all schemas will be migrated.")
	composedCmd.Flags().StringVarP(&privateKey, "private-key", "k", "", "Private key to use for migration")
	composedCmd.Flags().StringVarP(&rpc, "rpc", "r", "", "RPC to use for migration")
	composedCmd.Flags().StringVarP(&taxonomyFile, "taxonomy-file", "f", "", "Taxonomy CSV file to use for migration")
	composedCmd.Flags().StringVarP(&schema, "schema", "c", "", "Schema file to use for migration")

	// mark the flags as required
	composedCmd.MarkFlagRequired("private-key")
	composedCmd.MarkFlagRequired("rpc")
	composedCmd.MarkFlagRequired("taxonomy-file")
	composedCmd.MarkFlagRequired("schema")

	return composedCmd
}

func migrateComposed(ctx context.Context, client ctypes.Client, schema *types.Schema, streamID string, children []child) error {
	// drop the stream
	_, err := client.DropDatabase(ctx, streamID, ctypes.WithSyncBroadcast(true))
	if err != nil {
		return err
	}

	schema.Name = streamID

	// migrate the stream
	_, err = client.DeployDatabase(ctx, schema, ctypes.WithSyncBroadcast(true))
	if err != nil {
		return err
	}

	// TODO: insert the child weight data into the stream
	return nil
}

// based on https://github.com/truflation/tsn-data-provider/blob/f0837831b36cbca4201252684ac9a19ae4ae0b1d/assets/categories/cpi_uk/taxonomy.csv
type TaxonomyData struct {
	ParentOf string
	Weight   *decimal.Decimal
	StreamID string
}

func readTaxonomyCSV(filePath string) ([]*TaxonomyData, error) {
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

	var data []*TaxonomyData

	// Process each record
	for _, record := range records {
		dec, err := decimal.NewFromString(record[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing weight: %w", err)
		}

		// Create a new TaxonomyData struct
		TaxonomyData := &TaxonomyData{
			ParentOf: record[0],
			Weight:   dec,
			// skip table field since it is not used
			StreamID: record[3],
		}

		// Append to the slice
		data = append(data, TaxonomyData)
	}

	return data, nil
}

// normalizeTaxonomyData removes duplicate streamIDs and groups them by parentOf.
func normalizeTaxonomyData(data []*TaxonomyData) []normalizedTaxonomyData {
	normalizedSet := make(map[string]normalizedTaxonomyData)

	for _, d := range data {
		if _, ok := normalizedSet[d.ParentOf]; !ok {
			normalizedSet[d.ParentOf] = normalizedTaxonomyData{
				StreamID: d.StreamID,
			}
		}

		v := normalizedSet[d.ParentOf]
		v.Children = append(v.Children, child{
			ID:     d.StreamID,
			Weight: d.Weight,
		})
	}

	var normalizedData []normalizedTaxonomyData
	for _, v := range normalizedSet {
		normalizedData = append(normalizedData, v)
	}

	return normalizedData
}

type normalizedTaxonomyData struct {
	StreamID string
	Children []child
}

type child struct {
	ID     string
	Weight *decimal.Decimal
}

// removeUnnecessaryTaxonomyData removes any streamIDs not in the given slice
func removeUnnecessaryTaxonomyData(data []normalizedTaxonomyData, streamIDs []string) []normalizedTaxonomyData {
	set := make(map[string]struct{}, len(streamIDs))
	for _, s := range streamIDs {
		set[s] = struct{}{}
	}

	var newData []normalizedTaxonomyData
	for _, d := range data {
		_, ok := set[d.StreamID]
		if ok {
			newData = append(newData, d)
		}
	}

	return newData
}
