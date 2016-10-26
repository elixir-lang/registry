# MIX_ENV=bench mix run bench/erlang.exs
# TASKS=8 MIX_ENV=bench mix run bench/erlang.exs

Code.require_file "shared.exs", __DIR__
Application.ensure_all_started(:gproc)

tasks = String.to_integer System.get_env("TASKS") || "1"
IO.puts "gproc: registering #{tasks} x 10000 entries"

names =
  for task <- 1..tasks do
    for i <- 1..10000, do: {:via, :gproc, {:n, :l, {task, i}}}
  end

:timer.tc(fn ->
  names
  |> Enum.map(&Task.async(Shared, :register, [&1]))
  |> Enum.each(&Task.await(&1, :infinity))
end) |> IO.inspect

Shared.check(names)
