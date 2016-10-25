# MIX_ENV=bench mix run bench/erlang.exs
# TASKS=8 MIX_ENV=bench mix run bench/erlang.exs
# PARTITIONS=8 TASKS=8 MIX_ENV=bench mix run bench/erlang.exs

Code.require_file "shared.exs", __DIR__
partitions = String.to_integer System.get_env("PARTITIONS") || "1"
Registry.start_link(:unique, Registry, partitions: partitions)

tasks = String.to_integer System.get_env("TASKS") || "1"

names =
  for task <- 1..tasks do
    for i <- 1..10000, do: {:via, Registry, {Registry, {task, i}}}
  end

:timer.tc(fn ->
  names
  |> Enum.map(&Task.async(Shared, :register, [&1]))
  |> Enum.each(&Task.await(&1, :infinity))
end) |> IO.inspect
