defmodule LoggerFluentdBackend.Sender do
  use GenServer

  alias Socket.Stream
  alias Socket.TCP

  require Logger

  defmodule State do
    defstruct socket: nil,
              connection_failure_warned: false
  end

  def start_link([]) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(_) do
    {:ok, %State{socket: nil}}
  end

  def send(tag, data, host, port, serializer) do
    options = [host: host, port: port, serializer: serializer]
    :ok = GenServer.cast(__MODULE__, {:send, tag, data, options})
  end

  def send(tag, data, host, port), do: send(tag, data, host, port, :msgpack)

  def stop() do
    GenServer.call(__MODULE__, {:stop, []})
  end

  def handle_call({:stop, _}, _from, _state) do
    {:reply, :ok, %State{socket: nil}}
  end

  def terminate(_reason, %State{socket: nil}), do: :ok

  def terminate(_reason, %State{socket: socket}) do
    Stream.close(socket)
  end

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def handle_cast({_, _, _, options} = msg, %State{socket: nil} = state) do
    case connect(options, state) do
      %State{socket: nil} = state ->
        {:noreply, state}

      state ->
        handle_cast(msg, state)
    end
  end

  def handle_cast({:send, tag, data, options}, %State{socket: socket} = state) do
    # Fluent-bit expects an EXT type for Forward input timestamp (10-bytes, w/ 4B epoch
    # seconds and 4B ns)
    now = now()

    {now_s, now_ms} =
      now
      |> Decimal.from_float()
      |> Decimal.div_rem(1)

    now_s = Decimal.to_integer(now_s)

    now_ns =
      now_ms
      |> Decimal.mult(1_000_000)
      |> Decimal.to_integer()

    # Insert D7 (fixext 8), 00 (integer type), and time (2-part integer).
    # spec for ref: https://github.com/msgpack/msgpack/blob/master/spec.md#formats
    # NOTE: must be big-endian (elixir bitstring default)
    time_bitstring =
      <<0xD7, 0x00>> <> <<now_s::unsigned-size(32)>> <> <<now_ns::unsigned-size(32)>>

    time_binary = Msgpax.unpack!(time_bitstring)
    payload = [tag, time_binary, data]

    packet = serializer(options[:serializer]).(payload)
    Stream.send!(socket, packet)
    {:noreply, state}
  end

  defp print_binary(bitstring) do
    for(<<x::size(1) <- bitstring>>, do: "#{x}")
    |> Enum.chunk_every(8)
    |> Enum.join(" ")
  end

  defp print_hex(bitstring) do
    bitstring
    |> Base.encode16()
    |> String.graphemes()
    |> Enum.chunk_every(2)
    |> Enum.join(" ")
  end

  # Try to connect a socket, returning the state with the resulting socket if successful
  @spec connect(keyword(), %State{}) :: %State{}
  defp connect(options, %{connection_failure_warned: warned}) do
    host = options[:host] || "localhost"
    port = options[:port] || 24224

    case TCP.connect(host, port, packet: 0) do
      {:ok, socket} ->
        %State{socket: socket, connection_failure_warned: false}

      {:error, err} ->
        if not warned do
          Logger.error(
            "Unable to connect TCP socket at #{host}:#{port} for fluent logger: #{err} "
          )
        end

        %State{socket: nil, connection_failure_warned: true}
    end
  end

  defp serializer(:msgpack), do: &Msgpax.pack!/1
  defp serializer(:json), do: &Jason.encode!/1
  defp serializer(f) when is_function(f, 1), do: f

  defp now() do
    {megasec, sec, usec} = :os.timestamp()
    megasec * 1_000_000 + sec + usec / 1_000_000
  end
end
