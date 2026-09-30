defmodule Dataflow.SQL do
  @moduledoc """
  SQL statement summarization for DB_QUERY spans: verb plus the first table
  reference as a short human name ("SELECT orders", "INSERT users"). Pure
  string work — parameter values are never captured; only the statement
  text the caller already sends is single-spaced and truncated for
  "db.statement" metadata.

  Mirrors the Go SDK's derivation so both SDKs render identical names.
  """

  # First match wins: the leading keyword of the whitespace-normalized
  # statement (a leading "(" wraps CTEs and still matches).
  @verb_re ~r/^\s*\(?\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN)\b/i

  # First FROM|INTO|UPDATE|TABLE|JOIN reference, skipping IF [NOT] EXISTS;
  # an optional quote char is consumed, so Ecto's `INSERT INTO "users"`
  # reports the bare table. Schema-qualified names ("public.items") report
  # the bare table.
  @table_re ~r/\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?["'`\[]?([A-Za-z_][\w$.]*)/i

  @max_statement 200

  @doc """
  Renders the span name for a statement: verb plus first table when one
  exists ("SELECT orders"); bare verbs report alone ("BEGIN") and non-SQL
  strings fall back to the first word upcased ("QUERY" when nothing usable).
  """
  def summary(query) when is_binary(query) do
    one = one_line(query)

    case Regex.run(@verb_re, one) do
      [_full, verb] ->
        verb = String.upcase(verb)

        case table_from(one) do
          nil -> verb
          table -> verb <> " " <> table
        end

      nil ->
        first_word(one)
    end
  end

  def summary(_query), do: "QUERY"

  @doc """
  Single-spaced statement text truncated to 200 chars, for the
  "db.statement" metadata entry. Ecto sends parameter values separately
  from the statement text, so this never carries bound values.
  """
  def clip_statement(query) when is_binary(query) do
    query |> one_line() |> String.slice(0, @max_statement)
  end

  def clip_statement(_query), do: ""

  defp one_line(query), do: query |> String.split(~r/\s+/, trim: true) |> Enum.join(" ")

  defp table_from(one) do
    case Regex.run(@table_re, one) do
      [_full, raw] ->
        case raw |> String.split([".", "$"]) |> List.last() do
          "" -> nil
          table -> table
        end

      nil ->
        nil
    end
  end

  defp first_word(one) do
    case String.split(one, " ", parts: 2) do
      [word | _] when word != "" ->
        word |> String.split("(") |> hd() |> String.upcase()

      _ ->
        "QUERY"
    end
  end
end
