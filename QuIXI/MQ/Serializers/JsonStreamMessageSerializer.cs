using IXICore;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;

namespace QuIXI.MQ.Serializers
{
    public class JsonStreamMessageSerializer : IMessageSerializer
    {
        public byte[] Serialize<T>(T obj)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj, new JsonSerializerSettings() { ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                Converters = new List<JsonConverter> { new ByteArrayDictionaryConverter<IxiNumber>() }
            }));
        }

        public T Deserialize<T>(byte[] data)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data));
        }
    }
}
