package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

func (e *ShowExec) fillSplitsToChunk(regions []regionMeta, tbInfo *model.TableInfo) {
	tableStart := tablecodec.GenTableRecordPrefix(tbInfo.ID)
	tableEnd := tableStart.PrefixNext()

	for _, region := range regions {
		regionMeta := region.region
		regionStart := kv.Key(regionMeta.StartKey)
		regionEnd := kv.Key(regionMeta.EndKey)
		if regionStart.Cmp(tableStart) < 0 {
			regionStart = tableStart
		}
		if regionEnd.Cmp(tableEnd) > 0 {
			regionEnd = tableEnd
		}

		e.result.AppendUint64(0, regionMeta.Id)
		e.result.AppendString(1, regionStart.String())
		e.result.AppendString(2, regionEnd.String())
		e.result.AppendInt64(3, region.approximateSize)
		e.result.AppendInt64(4, region.approximateKeys)
	}
}

type noIndexTableWrapper struct {
	table.Table
	noIndexMeta *model.TableInfo
}

func noIndexTable(t table.Table) table.Table {
	noIndexMeta := t.Meta().Clone()
	noIndexMeta.Indices = nil
	return &noIndexTableWrapper{
		Table:       t,
		noIndexMeta: noIndexMeta,
	}
}

func (t *noIndexTableWrapper) Meta() *model.TableInfo {
	return t.noIndexMeta
}

func (e *ShowExec) fetchShowTableSplits(ctx context.Context) error {
	store := e.Ctx().GetStore()
	tikvStore, ok := store.(helper.Storage)
	if !ok {
		return nil
	}
	splitStore, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	physicalIDs := []int64{}
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Table.PartitionNames {
			pid, err := tables.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
	} else {
		if len(e.Table.PartitionNames) != 0 {
			return plannercore.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
	}

	// Get table regions from from pd, not from regionCache, because the region cache maybe outdated.
	var regions []regionMeta
	regions, err = getTableRegions(noIndexTable(tb), physicalIDs, tikvStore, splitStore)
	if err != nil {
		return err
	}

	e.fillSplitsToChunk(regions, tb.Meta())
	return nil
}
