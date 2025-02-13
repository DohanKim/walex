defmodule WalEx.Config.RegistryTest do
  use ExUnit.Case, async: false

  require Logger

  alias WalEx.Supervisor, as: WalExSupervisor
  alias WalEx.Config.Registry, as: WalExRegistry
  alias WalEx.Config.RegistryTest, as: WalExRegistryTest

  @base_configs [
    name: :test_name,
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "todos_test",
    port: 5432,
    subscriptions: ["subscriptions"],
    publication: "events"
  ]

  describe "start_registry/0" do
    test "should start a process" do
      assert {:ok, pid} = WalExRegistry.start_registry()
      assert is_pid(pid)
    end
  end

  describe "set_name/3" do
    setup do
      {:ok, _pid} = WalExRegistry.start_registry()
      :ok
    end

    test "should set agent name" do
      assert {:via, Registry, {:walex_registry, {WalExRegistryTest, :app_name_test}}} ==
               WalExRegistry.set_name(:set_agent, __MODULE__, :app_name_test)
    end

    test "should set get server name" do
      assert {:via, Registry, {:walex_registry, {WalExRegistryTest, :app_name_test}}} ==
               WalExRegistry.set_name(:set_gen_server, __MODULE__, :app_name_test)
    end

    test "should set supervisor name" do
      assert {:via, Registry, {:walex_registry, {WalExRegistryTest, :app_name_test}}} ==
               WalExRegistry.set_name(:set_supervisor, __MODULE__, :app_name_test)
    end

    test "should be able to find processes" do
      assert {:ok, walex_supervisor_pid} = WalExSupervisor.start_link(@base_configs)

      assert walex_supervisor_pid ==
               GenServer.whereis(
                 WalExRegistry.set_name(:set_supervisor, WalExSupervisor, :test_name)
               )

      assert GenServer.whereis(
               WalExRegistry.set_name(
                 :set_supervisor,
                 WalEx.Events.Supervisor,
                 :test_name
               )
             )
             |> is_pid()

      assert GenServer.whereis(
               WalExRegistry.set_name(
                 :set_supervisor,
                 WalEx.Replication.Supervisor,
                 :test_name
               )
             )
             |> is_pid()

      assert GenServer.whereis(WalExRegistry.set_name(:set_gen_server, WalEx.Events, :test_name))
             |> is_pid()

      assert GenServer.whereis(
               WalExRegistry.set_name(:set_gen_server, WalEx.Replication.Server, :test_name)
             )
             |> is_pid()

      assert GenServer.whereis(
               WalExRegistry.set_name(:set_gen_server, WalEx.Replication.Publisher, :test_name)
             )
             |> is_pid()
    end
  end

  describe "get_state/3" do
    setup do
      {:ok, _pid} = WalExRegistry.start_registry()
      :ok
    end

    test "should set agent state" do
      name = WalExRegistry.set_name(:set_agent, __MODULE__, :app_name_test)

      configs = []
      Agent.start_link(fn -> configs end, name: name)

      assert configs == WalExRegistry.get_state(:get_agent, __MODULE__, :app_name_test)
    end
  end

  describe "find_pid" do
    test "should find Supervisor" do
      assert {:ok, walex_supervisor_pid} = WalExSupervisor.start_link(@base_configs)

      assert walex_supervisor_pid ==
               GenServer.whereis(
                 WalEx.Config.Registry.set_name(:set_supervisor, WalEx.Supervisor, :test_name)
               )
    end
  end
end
