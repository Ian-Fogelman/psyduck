#define DUCKDB_EXTENSION_MAIN

#include "psyduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <openssl/opensslv.h>
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "pokemon_data.hpp"
#include "pokemon_moves_data.hpp"
#include "pokemon_items_data.hpp"

namespace duckdb {

// Structure to hold bind data for psyduck_subscribe
struct ListPokemonBindData : public TableFunctionData {
	explicit ListPokemonBindData() {
	}
	bool indicator = false;
};

static unique_ptr<FunctionData> ListPokemonBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("number");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("name");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("type1");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("type2");

	return_types.emplace_back(LogicalType::FLOAT);
	names.emplace_back("height(m)");

	return_types.emplace_back(LogicalType::FLOAT);
	names.emplace_back("weight(kg)");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("base_total");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("hp");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("attack");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("defense");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("special");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("speed");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("evolutions");

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("is_legendary");

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("is_duck");

	return make_uniq<ListPokemonBindData>();
}

void ListPokemon(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<ListPokemonBindData>();

	if (bind_data.indicator) {
		output.SetCardinality(0);
		return;
	}
	bind_data.indicator = true;

	// Set output to 151 rows
	output.SetCardinality(pokemon.size());

	for (idx_t i = 0; i < pokemon.size(); i++) {
		FlatVector::GetData<uint64_t>(output.data[0])[i] = pokemon[i].number;
		FlatVector::GetData<duckdb::string_t>(output.data[1])[i] = duckdb::string_t(pokemon[i].name);
		FlatVector::GetData<duckdb::string_t>(output.data[2])[i] = duckdb::string_t(pokemon[i].type1);
		FlatVector::GetData<duckdb::string_t>(output.data[3])[i] = duckdb::string_t(pokemon[i].type2);
		FlatVector::GetData<float>(output.data[4])[i] = pokemon[i].height;
		FlatVector::GetData<float>(output.data[5])[i] = pokemon[i].weight;
		FlatVector::GetData<int32_t>(output.data[6])[i] = pokemon[i].base_total;
		FlatVector::GetData<int32_t>(output.data[7])[i] = pokemon[i].hp;
		FlatVector::GetData<int32_t>(output.data[8])[i] = pokemon[i].attack;
		FlatVector::GetData<int32_t>(output.data[9])[i] = pokemon[i].defense;
		FlatVector::GetData<int32_t>(output.data[10])[i] = pokemon[i].special;
		FlatVector::GetData<int32_t>(output.data[11])[i] = pokemon[i].speed;
		FlatVector::GetData<int32_t>(output.data[12])[i] = pokemon[i].evolutions;
		FlatVector::GetData<bool>(output.data[13])[i] = pokemon[i].is_legendary;
		FlatVector::GetData<bool>(output.data[14])[i] = pokemon[i].is_duck;
	}
}

// List Pokemon Moves Begin
struct ListPokemonMovesBindData : public TableFunctionData {
	explicit ListPokemonMovesBindData() {
	}
	bool indicator = false;
};

static unique_ptr<FunctionData> ListPokemonMovesBind(ClientContext &, TableFunctionBindInput &,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("name");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("type");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("power");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("accuracy");

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("pp");

	return make_uniq<ListPokemonBindData>(); // reuse the same struct
}

void ListPokemonMoves(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	// Reuse the same bind pattern as ListPokemon
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<ListPokemonBindData>(); // works fine, name doesn't matter

	if (bind_data.indicator) {
		output.SetCardinality(0);
		return;
	}
	bind_data.indicator = true;

	// 165 moves in Gen 1
	output.SetCardinality(moves.size());

	for (idx_t i = 0; i < moves.size(); ++i) {
		const auto &move = moves[i];

		FlatVector::GetData<string_t>(output.data[0])[i] = string_t(move.name); // name
		FlatVector::GetData<string_t>(output.data[1])[i] = string_t(move.type); // type
		FlatVector::GetData<int32_t>(output.data[2])[i] = move.power;           // power (-1 = status)
		FlatVector::GetData<int32_t>(output.data[3])[i] = move.accuracy;        // accuracy (-1 = never misses)
		FlatVector::GetData<int32_t>(output.data[4])[i] = move.pp;              // pp
	}
}
// List Pokemon Moves End

// List Pokemon Items Begin
struct ListPokemonItemsBindData : public TableFunctionData {
	explicit ListPokemonItemsBindData() {
	}
	bool indicator = false;
};

static unique_ptr<FunctionData> ListPokemonItemsBind(ClientContext &, TableFunctionBindInput &,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("id");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("category");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("location");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("price");
	return make_uniq<ListPokemonItemsBindData>();
}

void ListPokemonItems(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(data_p.bind_data);
	auto &bind_data = data_p.bind_data->CastNoConst<ListPokemonItemsBindData>();
	if (bind_data.indicator) {
		output.SetCardinality(0);
		return;
	}
	bind_data.indicator = true;
	// 127 items in Gen 1
	output.SetCardinality(items.size());
	for (idx_t i = 0; i < items.size(); ++i) {
		const auto &item = items[i];
		FlatVector::GetData<int32_t>(output.data[0])[i] = item.id;                     // id
		FlatVector::GetData<string_t>(output.data[1])[i] = string_t(item.name);        // name
		FlatVector::GetData<string_t>(output.data[2])[i] = string_t(item.category);    // category
		FlatVector::GetData<string_t>(output.data[3])[i] = string_t(item.description); // description
		FlatVector::GetData<string_t>(output.data[4])[i] = string_t(item.location);    // location
		FlatVector::GetData<int32_t>(output.data[5])[i] = item.price;                  // price
	}
}
// List Pokemon Items End

static void LoadInternal(ExtensionLoader &loader) {
	auto list_pokemon = TableFunction("list_pokemon", {}, ListPokemon, ListPokemonBind);
	auto list_pokemon_moves = TableFunction("list_pokemon_moves", {}, ListPokemonMoves, ListPokemonMovesBind);
	auto list_pokemon_items = TableFunction("list_pokemon_items", {}, ListPokemonItems, ListPokemonItemsBind);
	loader.RegisterFunction(list_pokemon);
	loader.RegisterFunction(list_pokemon_moves);
	loader.RegisterFunction(list_pokemon_items);
}

void PsyduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string PsyduckExtension::Name() {
	return "psyduck";
}

std::string PsyduckExtension::Version() const {
#ifdef EXT_VERSION_PSYDUCK
	return EXT_VERSION_PSYDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(psyduck, loader) {
	duckdb::LoadInternal(loader);
}
}
