GO ?= go
GO_BUILD ?= $(GO) build

rec_wildcard = $(foreach d,$(wildcard $1*),$(call rec_wildcard,$d/,$2) $(filter $(subst *,%,$2),$d))
SRC := $(call rec_wildcard,,*.go) go.mod go.sum

## build the binary for CLI
cmd/import-dynamodb/import-dynamodb: $(SRC)
	cd cmd/import-dynamodb/ && \
	$(GO_BUILD) -o import-dynamodb main.go
