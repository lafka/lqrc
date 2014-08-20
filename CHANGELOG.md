# Changelog


## 0.2.0

- Backwards incompattible changes

	- Strict dependency on Elixir 0.15 or greater.
	- Migrate to maps, proplists can still be used but input will be
	  converted to a map
	- Rename `LQRC.Schema.validate` -> `LQRC.Schema.valid?`
