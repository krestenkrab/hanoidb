REBAR = ./rebar
DIALYZER = dialyzer

.PHONY: plt analyze all deps compile get-deps clean

all: compile

get-deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

eunit: compile clean-test-btrees
	@$(REBAR) eunit skip_deps=true

eunit_console:
	erl -pa .eunit deps/*/ebin

clean-test-btrees:
	rm -fr .eunit/Btree_* .eunit/simple

plt: compile
	$(DIALYZER) --build_plt --output_plt .fractal_btree.plt \
		-pa deps/plain_fsm/ebin \
		-pa deps/ebloom/ebin \
		deps/plain_fsm/ebin \
		deps/ebloom/ebin \
		--apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .fractal_btree.plt \
	-pa deps/plain_fsm/ebin \
	-pa deps/ebloom/ebin \
	ebin

