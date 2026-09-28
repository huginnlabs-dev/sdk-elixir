defmodule Dataflow.Pii do
  @moduledoc """
  Client-side PII classification: payload field names → privacy category
  labels. Only labels travel in metadata; values stay in the E2E-encrypted
  payload.
  """

  @categories [
    {"password", ~w(password passwd pwd)},
    {"secret", ~w(token secret apikey api_key credential session jwt auth)},
    {"payment", ~w(card pan cvv cvc iban expiry)},
    {"email", ~w(email e_mail mail)},
    {"phone", ~w(phone mobile tel msisdn)},
    {"government_id", ~w(ssn passport tax_id national_id)},
    {"birth", ~w(birth dob age)},
    {"name", ~w(first_name last_name full_name surname customer_name display_name)},
    {"address", ~w(street zip postal street_address postal_address home_address billing_address shipping_address mailing_address)},
    {"geo", ~w(city country region location lat lon lng)},
    {"ip", ~w(ip ip_address client_ip remote_addr)},
    {"device", ~w(device user_agent imei fingerprint)}
  ]

  @doc "Maps field names to deduplicated, comma-joined privacy categories."
  def classify(fields) do
    seen =
      for field <- fields, reduce: MapSet.new() do
        acc ->
          norm =
            field
            |> String.downcase()
            |> String.replace(~r/[^a-z0-9_]+/, "_")

          tokens = MapSet.new(String.split(norm, "_"))

          hit =
            Enum.find_value(@categories, fn {cat, keywords} ->
              if MapSet.member?(acc, cat), do: nil, else: match_keyword(keywords, norm, tokens, cat)
            end)

          case hit do
            nil -> acc
            cat -> MapSet.put(acc, cat)
          end
      end

    seen |> Enum.sort() |> Enum.join(",")
  end

  defp match_keyword(keywords, norm, tokens, cat) do
    Enum.find_value(keywords, fn kw ->
      hit = if String.contains?(kw, "_"), do: String.contains?(norm, kw), else: MapSet.member?(tokens, kw)
      if hit, do: cat
    end)
  end
end
