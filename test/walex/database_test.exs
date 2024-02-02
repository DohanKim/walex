defmodule WalEx.DatabaseTest do
  use ExUnit.Case, async: false

  alias WalEx.Supervisor, as: WalExSupervisor

  require Logger

  @hostname "localhost"
  @username "postgres"
  @password "postgres"
  @database "todos_test"

  @base_configs [
    name: :todos,
    hostname: @hostname,
    username: @username,
    password: @password,
    database: @database,
    port: 5432,
    subscriptions: ["user", "todo"],
    publication: "events"
  ]

  describe "logical replication" do
    setup do
      {:ok, database_pid} = start_database()

      %{database_pid: database_pid}
    end

    test "should have logical replication set up", %{database_pid: pid} do
      show_wall_level = "SHOW wal_level;"

      assert is_pid(pid)
      assert [%{"wal_level" => "logical"}] == query(pid, show_wall_level)
    end

    test "should start replication slot", %{database_pid: database_pid} do
      assert {:ok, replication_pid} = WalExSupervisor.start_link(@base_configs)
      assert is_pid(replication_pid)

      pg_replication_slots = "SELECT slot_name, slot_type, active FROM \"pg_replication_slots\";"

      assert [
               %{"active" => true, "slot_name" => slot_name, "slot_type" => "logical"}
               | _replication_slots
             ] = query(database_pid, pg_replication_slots)

      assert String.contains?(slot_name, "walex_temp_slot")
    end

    test "should re-initiate replication slot", %{database_pid: database_pid} do
      {:ok, supervisor_pid} = TestSupervisor.start_link()

      database_pid =
        Supervisor.which_children(supervisor_pid)
        |> tap(&Logger.debug("Children" <> inspect(&1)))
        |> Enum.find(&match?({DBConnection.ConnectionPool, _, _, _}, &1))
        |> elem(1)
        |> tap(&Logger.debug("Database pid" <> inspect(&1)))

      pg_replication_slots = "SELECT slot_name, slot_type, active FROM \"pg_replication_slots\";"

      query(database_pid, pg_replication_slots)
      |> tap(&Logger.debug("Replication slots" <> inspect(&1)))

      name =
        WalEx.Config.Registry.set_name(:set_gen_server, WalEx.Replication.Server, :todos)
        |> tap(&Logger.debug("Server name" <> inspect(&1)))

      replication_server_pid =
        GenServer.whereis(name)
        |> tap(&Logger.debug("Server pid" <> inspect(&1)))

      # {output, exit_code} = System.cmd("sudo", ["service", "postgresql", "restart"])

      Process.info(database_pid) |> tap(&Logger.debug("Database pid" <> inspect(&1)))

      Supervisor.terminate_child(supervisor_pid, database_pid)
      |> tap(&Logger.debug("Terminated" <> inspect(&1)))

      Supervisor.delete_child(supervisor_pid, database_pid)
      |> tap(&Logger.debug("Deleted" <> inspect(&1)))

      Supervisor.which_children(supervisor_pid)
      |> tap(&Logger.debug("Children" <> inspect(&1)))

      # Process.exit(database_pid, :kill)

      :timer.sleep(3000)

      Supervisor.which_children(supervisor_pid)
      |> tap(&Logger.debug("Children" <> inspect(&1)))

      database_pid =
        Supervisor.which_children(supervisor_pid)
        |> tap(&Logger.debug("Children" <> inspect(&1)))
        |> Enum.find(&match?({DBConnection.ConnectionPool, _, _, _}, &1))
        |> elem(1)

      query(database_pid, pg_replication_slots)
      |> tap(&Logger.debug("Replication slots" <> inspect(&1)))

      # assert [
      #          %{"active" => true, "slot_name" => slot_name, "slot_type" => "logical"}
      #          | _replication_slots
      #        ] = query(database_pid, pg_replication_slots)

      # assert String.contains?(slot_name, "walex_temp_slot")
    end
  end

  def start_database do
    Postgrex.start_link(
      hostname: @hostname,
      username: @username,
      password: @password,
      database: @database
    )
  end

  def query(pid, query) do
    pid
    |> Postgrex.query!(query, [])
    |> map_rows_to_columns()
  end

  def map_rows_to_columns(%Postgrex.Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row -> Enum.zip(columns, row) |> Map.new() end)
  end

  def map_rows_to_columns(_result), do: []
end

defmodule TestSupervisor do
  use Supervisor

  @hostname "localhost"
  @username "postgres"
  @password "postgres"
  @database "todos_test"

  @base_configs [
    name: :todos,
    hostname: @hostname,
    username: @username,
    password: @password,
    database: @database,
    port: 5432,
    subscriptions: ["user", "todo"],
    publication: "events"
  ]

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      {Postgrex,
       [hostname: @hostname, username: @username, password: @password, database: @database]},
      {WalEx.Supervisor, @base_configs}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
