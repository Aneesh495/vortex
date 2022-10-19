# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

.PHONY: all build test test-py bench clean run-broker lint-emdash

all: build test test-py

build:
	dotnet build Vortex.slnx

test:
	dotnet test --verbosity normal

test-py:
	cd sdk/vortex-py && python3 -m unittest discover -s tests

bench:
	dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -c Release -- bench 100000 256

run-broker:
	dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -- topic list

clean:
	dotnet clean
	rm -rf tests/Vortex.Tests/TestResults
	rm -rf /tmp/vortex-data*

lint-emdash:
	@echo "Checking for illegal em dashes in source and documentation..."
	@! grep -rn --exclude-dir={bin,obj,.git} "$$(printf '\xe2\x80\x94')" . || (echo "Error: Em dashes found!" && exit 1)
	@echo "Verification passed: zero em dashes found across all repository files."
