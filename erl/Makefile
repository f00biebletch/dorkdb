ERL          ?= erl
EBIN_DIRS    := $(wildcard deps/*/ebin)
APP          := dork

all: erl $(APP).app
prod: clean al

erl:
	@$(ERL) -pa $(EBIN_DIRS) -noinput +B \
	  -eval 'case make:all() of up_to_date -> halt(0); error -> halt(1) end.'

docs:
	@erl -noshell -run edoc_run application '$(APP)' '"."' '[]'

clean: 
	@echo "removing:"
	@rm -fv *.beam ebin/*.app


