package utils

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/urfave/cli/v2"
)

var (
	EnableInjectionFlag = &cli.BoolFlag{
		Name:     "enable-injection",
		Usage:    "Enable error injection to the database",
		Category: flags.MiscCategory,
	}
	ErrorTypeFlag = &cli.IntFlag{
		Name:     "error-type",
		Usage:    "The type of error to inject",
		Category: flags.MiscCategory,
	}
	FailWhenFlag = &cli.UintFlag{
		Name:     "inject-count",
		Usage:    "Which access to inject the error",
		Category: flags.MiscCategory,
	}
)

func SetErrorInjectionConfig(ctx *cli.Context, cfg *ethdb.ErrorInjectionConfig) {
	if ctx.Bool(EnableInjectionFlag.Name) {
		cfg.EnableInjection = true
		cfg.ErrorType = ethdb.ErrorType(ctx.Int(ErrorTypeFlag.Name))
		cfg.FailWhen = uint(ctx.Uint(FailWhenFlag.Name))
	}
}
