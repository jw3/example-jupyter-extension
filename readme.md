Jupyter Magic
===

Demonstrate implementing Jupyter magic extensions that connect to a Rust backend to operate on AIS vessel data.

- `src` - contains rust backend implementation
- `python` - contains Python Magic impl

Builds with Maturin `maturn develop`
Installs with pip `pip install git+https://github.com/jw3/example-jupyter-extension`

See the Containerfile for full deploy reference


## Reference

- https://github.com/jw3/meos-rs
- https://github.com/jw3/keplerize
- https://ipython.readthedocs.io/en/stable/config/custommagics.html

## License

GPL v3
