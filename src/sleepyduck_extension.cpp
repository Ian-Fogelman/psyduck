#define DUCKDB_EXTENSION_MAIN

#include "sleepyduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void SleepyduckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Sleepyduck " + name.GetString() + " 🐥");
	});
}

inline void SleepyduckOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Sleepyduck " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto sleepyduck_scalar_function =
	    ScalarFunction("sleepyduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SleepyduckScalarFun);
	loader.RegisterFunction(sleepyduck_scalar_function);

	// Register another scalar function
	auto sleepyduck_openssl_version_scalar_function = ScalarFunction(
	    "sleepyduck_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SleepyduckOpenSSLVersionScalarFun);
	loader.RegisterFunction(sleepyduck_openssl_version_scalar_function);
}

void SleepyduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string SleepyduckExtension::Name() {
	return "sleepyduck";
}

std::string SleepyduckExtension::Version() const {
#ifdef EXT_VERSION_SLEEPYDUCK
	return EXT_VERSION_SLEEPYDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sleepyduck, loader) {
	duckdb::LoadInternal(loader);
}
}
