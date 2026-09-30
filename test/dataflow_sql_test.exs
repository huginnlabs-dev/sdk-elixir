defmodule Dataflow.SQLTest do
  use ExUnit.Case, async: true

  describe "summary/1" do
    test "verb + first FROM table" do
      assert Dataflow.SQL.summary("SELECT * FROM orders WHERE id = 1") == "SELECT orders"
    end

    test "case-insensitive with schema-qualified table reporting the bare table" do
      assert Dataflow.SQL.summary("select id from public.items where id = $1") == "SELECT items"
    end

    test "INSERT INTO" do
      assert Dataflow.SQL.summary("INSERT INTO users (name, email) VALUES ($1, $2)") == "INSERT users"
    end

    test "CREATE TABLE IF NOT EXISTS skips the existence clause" do
      assert Dataflow.SQL.summary("CREATE TABLE IF NOT EXISTS audit_log (id integer)") == "CREATE audit_log"
    end

    test "DROP TABLE IF EXISTS skips the existence clause" do
      assert Dataflow.SQL.summary("DROP TABLE IF EXISTS old_things") == "DROP old_things"
    end

    test "UPDATE doubles as verb and table keyword" do
      assert Dataflow.SQL.summary("UPDATE users SET name = $1 WHERE id = $2") == "UPDATE users"
    end

    test "DELETE FROM" do
      assert Dataflow.SQL.summary("delete from events where id = $1") == "DELETE events"
    end

    test "quoted table name loses its quotes" do
      assert Dataflow.SQL.summary(~s(INSERT INTO "users" (name) VALUES ($1))) == "INSERT users"
    end

    test "multi-line statements are whitespace-normalized" do
      assert Dataflow.SQL.summary("SELECT *\n  FROM orders\n WHERE id = 1") == "SELECT orders"
    end

    test "leading parenthesis (wrapped statement) still matches the verb" do
      assert Dataflow.SQL.summary("(SELECT * FROM orders)") == "SELECT orders"
    end

    test "first FROM wins over later JOINs" do
      assert Dataflow.SQL.summary("SELECT * FROM a JOIN b ON b.a_id = a.id") == "SELECT a"
    end

    test "bare verb reports alone" do
      assert Dataflow.SQL.summary("BEGIN") == "BEGIN"
      assert Dataflow.SQL.summary("COMMIT") == "COMMIT"
    end

    test "unknown keyword falls back to the first word, upcased" do
      assert Dataflow.SQL.summary("PRAGMA journal_mode = WAL") == "PRAGMA"
    end

    test "empty and non-SQL input yield QUERY" do
      assert Dataflow.SQL.summary("") == "QUERY"
      assert Dataflow.SQL.summary("   ") == "QUERY"
      assert Dataflow.SQL.summary(nil) == "QUERY"
    end
  end

  describe "clip_statement/1" do
    test "collapses whitespace to single spaces" do
      assert Dataflow.SQL.clip_statement("SELECT *\n  FROM orders\tWHERE id = 1") == "SELECT * FROM orders WHERE id = 1"
    end

    test "truncates to 200 characters" do
      clipped = Dataflow.SQL.clip_statement(String.duplicate("x ", 300))
      assert String.length(clipped) == 200
    end

    test "short statements pass through unchanged" do
      assert Dataflow.SQL.clip_statement("SELECT 1") == "SELECT 1"
    end

    test "non-binary input yields an empty string" do
      assert Dataflow.SQL.clip_statement(nil) == ""
    end
  end
end
