# Setup Your Editor for Auto-completion and Instantaneous Validation

## Visual Studio Code (VSCode)
- Install the [YAML][1] plugin.
- Open `ocp-build-data` project.
- Add the following config options to `.vscode/settings.json`:
    ```json
    {
        "yaml.schemas": {
            "/path/to/json_schemas/releases.schema.json": "/releases.yml",
            "/path/to/json_schemas/image_config.schema.json": "/images/*.yml",
        }
    }
    ```
- Open the .yml file that you want to edit.

## PyCharm (or other Intellij IDEA based IDEs)
- Open `ocp-build-data` project.
- Go to `Preferences | Languages & Frameworks | Schemas and DTDs | JSON Schema Mappings`.
- Click `+`. Add a JSON schema mapping as the following:
  - *Name* Choose a friendly name. e.g. `OCP releases`
  - *Schema file or URL* The path to `json_schemas/releases.schema.json`
  - *Schema version* Choose `JSON Schema version 7`
  - Click `+`, choose `Add file`, type `releases.yml`, and press `Return`.
- Open `releases.yml`.

## Vim
- Install [Node.js][2].
- Install [coc-nvim][3] plugin for `vim`.
- Install `coc-yaml` server extension from within vim:
  ```
  :CocInstall coc-yaml
  ```
- Edit `coc-nvim` configuration
  ```
  :CocConfig
  ```
- Add the following config options:
  ```yaml
  {
        "yaml.schemas": {
            "/path/to/json_schemas/releases.schema.json": "/releases.yml",
            "/path/to/json_schemas/image_config.schema.json": "/images/*.yml",
        }
  }
  ```
- Open the .yml file that you want to edit.


[1]: https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml
[2]: https://nodejs.org/en/download/package-manager/
[3]: https://github.com/neoclide/coc.nvim/wiki/Install-coc.nvim
