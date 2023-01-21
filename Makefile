black:
	black --line-length 79 --experimental-string-processing --extend-exclude '/(\.git | \.toml | \.hg | \.toml | \.hg | \.eggs | \__pycache__ | \.csv | \.parquet | \.txt | \.rst | \.tox | \.nox | \.venv | _build | buck-out | build | dist)/' .
