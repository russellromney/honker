defmodule Honker.Extension do
  @moduledoc """
  Locates the Honker SQLite loadable extension.

  Hex packages cannot ship a compiled shared library the way the
  Python, Ruby, .NET, and JVM packages do, so unlike those bindings
  there is nothing bundled to fall back on. Set `HONKER_EXTENSION_PATH`,
  or download the extension from a Honker release:

      https://github.com/russellromney/honker/releases

  Use it to fill in `:extension_path` for `Honker.open/2`, or to load
  Honker onto an Exqlite connection you already own — which is what you
  want with Ecto, since enqueueing outside your transaction loses
  atomicity.

      {:ok, path} = Honker.Extension.path()
      Honker.open("app.db", extension_path: path)
  """

  @typedoc "Absolute path to the extension shared library."
  @type path :: String.t()

  @doc """
  The SQLite entry point exported by the extension.

  SQLite normally derives this from the file name — strip a leading
  `lib`, take characters up to the first `.`, keep the alphabetic ones,
  so `libhonker_ext.so` gives `honkerext`. It only matters when loading
  the extension under some other name.
  """
  @spec entrypoint() :: String.t()
  def entrypoint, do: "sqlite3_honkerext_init"

  @doc "The extension's file name on this platform."
  @spec filename() :: String.t()
  def filename do
    case :os.type() do
      {:win32, _} -> "honker_ext.dll"
      {:unix, :darwin} -> "libhonker_ext.dylib"
      _ -> "libhonker_ext.so"
    end
  end

  @doc """
  Locate the extension.

  Resolution order is `HONKER_EXTENSION_PATH`, then the current working
  directory, then `priv/` of the `:honker` application. Returns
  `{:error, message}` naming every path tried when none exists; nothing
  is guessed.
  """
  @spec path() :: {:ok, path()} | {:error, String.t()}
  def path do
    case System.get_env("HONKER_EXTENSION_PATH") do
      nil ->
        search()

      "" ->
        search()

      env ->
        if file?(env),
          do: {:ok, env},
          else: {:error, "HONKER_EXTENSION_PATH does not exist: #{env}"}
    end
  end

  @doc """
  Same as `path/0` but raises instead of returning an error tuple.
  """
  @spec path!() :: path()
  def path! do
    case path() do
      {:ok, p} -> p
      {:error, message} -> raise ArgumentError, message
    end
  end

  defp search do
    candidates = Enum.map(search_dirs(), &Path.join(&1, filename()))

    case Enum.find(candidates, &file?/1) do
      nil ->
        {:error,
         "Honker SQLite extension not found. The Elixir binding ships no binary — " <>
           "set HONKER_EXTENSION_PATH, or download honker-ext-<target>.tar.gz from " <>
           "https://github.com/russellromney/honker/releases and extract " <>
           "#{filename()} from it, or build it with " <>
           "`cargo build --release -p honker-extension`. Keep the file name as-is: " <>
           "SQLite derives the extension entry point from it. Searched: " <>
           Enum.join(candidates, ", ")}

      found ->
        {:ok, found}
    end
  end

  defp search_dirs do
    priv =
      case :code.priv_dir(:honker) do
        {:error, _} -> []
        dir -> [List.to_string(dir)]
      end

    [File.cwd!() | priv]
  end

  defp file?(path), do: File.regular?(path)
end
