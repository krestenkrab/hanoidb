REBAR=		rebar
DIALYZER=	dialyzer


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
		-pa deps/plain_fsm/ebin \
		-pa deps/ebloom/ebin \
		deps/plain_fsm/ebin \
		deps/ebloom/ebin \
		--apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .hanoi.plt \
	-pa deps/plain_fsm/ebin \
	-pa deps/ebloom/ebin \
	ebin

repl:
	erl -pz deps/*/ebin -pa ebin
