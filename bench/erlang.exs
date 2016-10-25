# MIX_ENV=bench mix run bench/erlang.exs
# TASKS=8 MIX_ENV=bench mix run bench/erlang.exs

Code.require_file "shared.exs", __DIR__

defmodule ErlangRegistry do
  def whereis_name(name) do
    :erlang.whereis(name)
  end

  def register_name(name, pid) when pid == self() do
    :erlang.register(name, pid)
    :yes
  end
end

tasks = String.to_integer System.get_env("TASKS") || "1"

names =
  for task <- 1..tasks do
    for i <- 1..10000, do: :"name-#{task}-#{i}"
  end

:timer.tc(fn ->
  names
  |> Enum.map(&Task.async(Shared, :register, [&1]))
  |> Enum.each(&Task.await(&1, :infinity))
end) |> IO.inspect
