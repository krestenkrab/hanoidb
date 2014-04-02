REBAR=		rebar
DIALYZER=	dialyzer

DEPS = $(CURDIR)/deps

DIALYZER_OPTS = # -Wunderspecs

# List dependencies that should be included in a cached dialyzer PLT file.
DIALYZER_DEPS = deps/lz4/ebin \
                deps/snappy/ebin \
                deps/plain_fsm/ebin

DEPS_PLT = hanoi.plt

ERLANG_DIALYZER_APPS = asn1 \
                       compiler \
                       crypto \
                       edoc \
                       edoc \
                       erts \
                       eunit \
                       eunit \
                       gs \
                       hipe \
                       inets \
                       kernel \
                       mnesia \
                       mnesia \
                       observer \
                       public_key \
                       runtime_tools \
                       runtime_tools \
                       ssl \
                       stdlib \
                       syntax_tools \
                       syntax_tools \
                       tools \
                       webtool \
                       xmerl


.PHONY: plt analyze all deps compile get-deps clean

all: compile

deps: get-deps

get-deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

test: eunit

eunit: compile clean-test-btrees
	@$(REBAR) eunit skip_deps=true

eunit_console:
	erl -pa .eunit deps/*/ebin

clean-test-btrees:
	rm -fr .eunit/Btree_* .eunit/simple

plt: compile
	$(DIALYZER) --build_plt --output_plt .hanoi.plt \
		-pa deps/snappy/ebin \
		-pa deps/plain_fsm/ebin \
		deps/plain_fsm/ebin \
		--apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .hanoi.plt \
	-pa deps/snappy/ebin \
	-pa deps/plain_fsm/ebin \
	ebin

analyze-nospec: compile
	$(DIALYZER) --plt .hanoi.plt \
	-pa deps/plain_fsm/ebin \
        --no_spec \
	ebin

# Only include local PLT if we have deps that we are going to analyze
ifeq ($(strip $(DIALYZER_DEPS)),)
dialyzer: ~/.dialyzer_plt compile
	@dialyzer $(DIALYZER_OPTS) -r ebin
else
dialyzer: ~/.dialyzer_plt $(DEPS_PLT) compile
	@dialyzer $(DIALYZER_OPTS) --plts ~/.dialyzer_plt $(DEPS_PLT) -r ebin

$(DEPS_PLT):
	@dialyzer --build_plt $(DIALYZER_DEPS) --output_plt $(DEPS_PLT)
endif

~/.dialyzer_plt:
	@echo "ERROR: Missing ~/.dialyzer_plt. Please wait while a new PLT is compiled."
	dialyzer --build_plt --apps $(ERLANG_DIALYZER_APPS)
	@echo "now try your build again"

repl:
	erl -pz deps/*/ebin -pa ebin
