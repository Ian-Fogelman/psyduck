# Clean everything
make clean
rm -rf build/

# Rebuild
GEN=ninja make release

# Pokemon Moves
https://gist.github.com/Ian-Fogelman/012ea77bd17686e95c6edf02ef652694

# Build
make

# Run 
./build/release/duckdb 

# Test
make test

# Format
make format-fix  

# Datasources
https://github.com/veekun/pokedex/tree/master/pokedex/data/csv

# Remove Git Cached
git rm --cached notes.md