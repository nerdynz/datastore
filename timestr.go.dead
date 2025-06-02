package datastore

import (
	"database/sql/driver"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
)

type TimeStr struct {
	pgtype.Timestamptz
}

func (t TimeStr) Scan(src any) error {
	return t.Timestamptz.Scan(src)
}

func (t TimeStr) Value() (driver.Value, error) {
	return t.Timestamptz.Value()
}

type TimeStrCodec struct {
	TimestamptzCodec pgtype.TimestamptzCodec
}

func (c *TimeStrCodec) FormatSupported(format int16) bool {
	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (c *TimeStrCodec) PreferredFormat() int16 {
	return pgtype.TextFormatCode
}

func (c *TimeStrCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.TimestamptzCodec.PlanEncode(m, oid, format, value)
}

func (c *TimeStrCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.TimestamptzCodec.PlanScan(m, oid, format, target)
}

func (c *TimeStrCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	val, err := c.TimestamptzCodec.DecodeDatabaseSQLValue(m, oid, format, src)
	if err != nil {
		return nil, err
	}
	slog.Info("DecodeDatabaseSQLValue", "val", val)
	return val, nil
}

func (c *TimeStrCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	val, err := c.TimestamptzCodec.DecodeValue(m, oid, format, src)
	if err != nil {
		return nil, err
	}
	slog.Info("DecodeValue", "val", val)
	return val, nil
}
