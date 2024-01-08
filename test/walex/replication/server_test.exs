defmodule WalEx.Replication.ServerTest do
  use ExUnit.Case, async: false
  # import WalEx.Support.TestHelpers

  alias WalEx.Supervisor, as: WalExSupervisor
  alias WalEx.Replication
  alias WalEx.ReplicationServer

  require Logger

  @app_name :test_app
  @hostname "localhost"
  @username "postgres"
  @password "postgres"
  @database "todos_test"

  @base_configs [
    name: @app_name,
    hostname: @hostname,
    username: @username,
    password: @password,
    database: @database,
    port: 5432,
    subscriptions: ["user", "todo"],
    publication: ["events"],
    modules: []
  ]

  describe "replication server" do
    @tag timeout: 300_000
    test "should reconnect to the database after disconnection" do
      {:ok, pid} =
        start_unlinked_database()

      assert is_pid(pid)

      assert {:ok, walex_supervisor_pid} = WalExSupervisor.start_link(@base_configs)

      :timer.sleep(300_000)
    end
  end

  def start_unlinked_database do
    parent = self()

    spawn(fn ->
      {:ok, conn_pid} =
        Postgrex.start_link(
          hostname: @hostname,
          username: @username,
          password: @password,
          database: @database
        )

      send(parent, {:postgrex_pid, conn_pid})
    end)

    receive do
      {:postgrex_pid, conn_pid} ->
        {:ok, conn_pid}
    after
      5000 ->
        {:error, :timeout}
    end
  end
end
