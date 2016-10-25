defmodule Shared do
  @compile :inline_list_funcs

  def register(names) do
    :lists.foreach(fn name ->
      Agent.start_link(fn -> [] end, name: name)
    end, names)
    :ok
  end
end
