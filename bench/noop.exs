# MIX_ENV=bench mix run bench/noop.exs
# TASKS=8 MIX_ENV=bench mix run bench/noop.exs

Code.require_file "shared.exs", __DIR__

defmodule NoopRegistry do
  def whereis_name(_name), do: :undefined
  def register_name(_name, pid) when pid == self(), do: :yes
end

tasks = String.to_integer System.get_env("TASKS") || "1"
IO.puts "noop: registering #{tasks} x 10000 entries"

names =
  for _ <- 1..tasks do
    for i <- 1..10000, do: {:via, NoopRegistry, i}
  end

:timer.tc(fn ->
  names
  |> Enum.map(&Task.async(Shared, :register, [&1]))
  |> Enum.each(&Task.await(&1, :infinity))
end) |> IO.inspect

Shared.check(names)
