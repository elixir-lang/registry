defmodule RegistryTest do
  use ExUnit.Case, asycn: true
  doctest Registry

  setup config do
    kind = config[:kind] || :unique
    partitions = config[:partitions] || 1
    {:ok, _} = Registry.start_link(kind, config.test, partitions: partitions)
    {:ok, %{registry: config.test, partitions: partitions}}
  end

  describe "unique" do
    @describetag kind: :unique
  end

  for {describe, partitions} <- ["with 1 partition": 1, "with 8 partitions": 8] do
    describe "unique #{describe}" do
      @describetag kind: :unique, partitions: partitions

      test "hello", config do

      end
    end
  end

  describe "duplicate" do
    @describetag kind: :duplicate
  end

  for {describe, partitions} <- ["with 1 partition": 1, "with 8 partitions": 8] do
    describe "duplicate #{describe}" do
      @describetag kind: :duplicate, partitions: partitions

      test "hello", config do

      end
    end
  end
end
