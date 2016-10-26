defmodule Shared do
  @compile :inline_list_funcs

  def register(names) do
    :lists.foreach(fn name ->
      {:ok, _} = Agent.start_link(fn -> [] end, name: name)
    end, names)
    :ok
  end

  def check(names) do
    IO.write "Checking... "
    missing =
      for name_list <- names,
          name <- name_list,
          !GenServer.whereis(name),
          do: :oops
    IO.puts "missing #{length missing} entries."
  end
end
